defmodule Keel.Log.Snapshot do
  @moduledoc "A snapshot file is renamed into place only after its bytes are synced"

  alias Keel.Common.T
  alias Keel.Env.Disk
  alias Keel.Raft.Config
  alias Keel.Raft.SnapshotMeta

  @magic <<"KEELSNP", 0>>
  @format_version 2
  @name ~r/\A(\d{20})-(\d{20})\.snap\z/

  @type header :: %{
          index: T.index(),
          term: T.term_number(),
          time_ms: non_neg_integer(),
          config: nil | Config.t(),
          payload: binary()
        }

  @type error :: :short | :bad_magic | :bad_version | :bad_length | :bad_crc

  @spec encode(T.index(), T.term_number(), non_neg_integer(), binary(), nil | Config.t()) ::
          binary()
  def encode(index, term, time_ms, payload, config \\ nil) do
    held = config_bytes(config)
    crc = held |> :erlang.crc32() |> :erlang.crc32(payload)

    <<@magic::binary, @format_version::16, index::64, term::64, time_ms::64, byte_size(held)::32,
      byte_size(payload)::64, crc::32, held::binary, payload::binary>>
  end

  @spec decode(binary()) :: {:ok, header()} | {:error, error()}
  def decode(bytes) do
    case bytes do
      <<"KEELSNP", 0, version::16, index::64, term::64, time_ms::64, config_length::32,
        length::64, crc::32, rest::binary>> ->
        header = %{index: index, term: term, time_ms: time_ms}

        check(version, {config_length, length}, crc, header, rest)

      <<magic::binary-size(8), _rest::binary>> when magic != @magic ->
        {:error, :bad_magic}

      _short ->
        {:error, :short}
    end
  end

  @doc "The meta of a whole file. Its size and checksum cover every byte of the file."
  @spec meta(binary()) :: {:ok, SnapshotMeta.t()} | {:error, error()}
  def meta(bytes) do
    with {:ok, header} <- decode(bytes) do
      meta = %SnapshotMeta{
        index: header.index,
        term: header.term,
        time_ms: header.time_ms,
        size: byte_size(bytes),
        checksum: :erlang.crc32(bytes),
        config: header.config
      }

      {:ok, meta}
    end
  end

  @spec verify(binary(), SnapshotMeta.t()) :: :ok | {:error, :mismatch | error()}
  def verify(bytes, %SnapshotMeta{} = expected) do
    case meta(bytes) do
      {:ok, ^expected} -> :ok
      {:ok, _other} -> {:error, :mismatch}
      {:error, _reason} = error -> error
    end
  end

  @spec name(T.index(), T.term_number()) :: String.t()
  def name(index, term), do: "#{pad(index)}-#{pad(term)}.snap"

  @spec path(Path.t(), SnapshotMeta.t()) :: Path.t()
  def path(dir, %SnapshotMeta{index: index, term: term}), do: Path.join(dir, name(index, term))

  @spec tmp_dir(Path.t()) :: Path.t()
  def tmp_dir(dir), do: Path.join(dir, "tmp")

  @spec part_path(Path.t(), SnapshotMeta.t()) :: Path.t()
  def part_path(dir, %SnapshotMeta{index: index, term: term}) do
    tmp = tmp_dir(dir)

    Path.join(tmp, "#{pad(index)}-#{pad(term)}.part")
  end

  @doc "The file is written under `tmp`, synced, renamed and the directory synced."
  @spec write(
          Disk.t(),
          Path.t(),
          T.index(),
          T.term_number(),
          non_neg_integer(),
          binary(),
          nil | Config.t()
        ) :: Disk.result(SnapshotMeta.t())
  def write(disk, dir, index, term, time_ms, payload, config \\ nil) do
    bytes = encode(index, term, time_ms, payload, config)
    {:ok, meta} = meta(bytes)

    with {:ok, stored} <- store(disk, dir, bytes, meta), do: {:ok, stored, meta}
  end

  @doc "Stores the bytes of a whole file the same way `write/6` does."
  @spec store(Disk.t(), Path.t(), binary(), SnapshotMeta.t()) :: Disk.result()
  def store(%module{} = disk, dir, bytes, %SnapshotMeta{} = meta) do
    part = part_path(dir, meta)

    with {:ok, made} <- ensure(disk, dir),
         {:ok, opened, handle} <- module.open(made, part, [:write, :create]),
         {:ok, written} <- module.write_at(opened, handle, 0, bytes),
         {:ok, synced} <- module.sync(written, handle),
         {:ok, closed} <- module.close(synced, handle) do
      install(closed, dir, meta)
    end
  end

  @doc "Moves the finished part file into place. Older snapshots are removed once the new name is durable."
  @spec install(Disk.t(), Path.t(), SnapshotMeta.t()) :: Disk.result()
  def install(%module{} = disk, dir, %SnapshotMeta{} = meta) do
    part = part_path(dir, meta)
    final = path(dir, meta)

    with {:ok, moved} <- module.rename(disk, part, final),
         {:ok, synced} <- module.sync_dir(moved, dir),
         {:ok, cleaned} <- module.sync_dir(synced, tmp_dir(dir)) do
      prune(cleaned, dir, meta)
    end
  end

  @doc "The newest file that is whole. A file that fails its checks is skipped."
  @spec latest(Disk.t(), Path.t()) :: Disk.result(nil | SnapshotMeta.t())
  def latest(%module{} = disk, dir) do
    case module.list(disk, dir) do
      {:ok, listed, names} -> newest(listed, dir, names)
      {:error, missing, :enoent} -> {:ok, missing, nil}
      {:error, _disk, _reason} = failed -> failed
    end
  end

  @spec read(Disk.t(), Path.t(), SnapshotMeta.t()) :: Disk.result(binary())
  def read(%module{} = disk, dir, %SnapshotMeta{} = meta) do
    with {:ok, read, bytes} <- module.read_file(disk, path(dir, meta)),
         :ok <- verify(bytes, meta),
         {:ok, header} <- decode(bytes) do
      {:ok, read, header.payload}
    else
      {:error, reason} -> {:error, disk, reason}
      {:error, _disk, _reason} = failed -> failed
    end
  end

  @doc "Removes every part file. They are never complete."
  @spec clean_tmp(Disk.t(), Path.t()) :: Disk.result()
  def clean_tmp(%module{} = disk, dir) do
    tmp = tmp_dir(dir)

    case module.list(disk, tmp) do
      {:ok, listed, []} -> {:ok, listed}
      {:ok, listed, names} -> delete_all(listed, tmp, names)
      {:error, missing, :enoent} -> {:ok, missing}
      {:error, _disk, _reason} = failed -> failed
    end
  end

  defp config_bytes(nil), do: <<>>
  defp config_bytes(%Config{} = config), do: Config.encode(config)

  defp check(@format_version, {config_length, length}, crc, header, rest)
       when byte_size(rest) == config_length + length do
    <<held::binary-size(^config_length), payload::binary>> = rest

    with true <- held |> :erlang.crc32() |> :erlang.crc32(payload) == crc,
         {:ok, config} <- config_of(held) do
      {:ok, Map.merge(header, %{config: config, payload: payload})}
    else
      _bad -> {:error, :bad_crc}
    end
  end

  defp check(@format_version, _lengths, _crc, _header, _rest), do: {:error, :bad_length}
  defp check(_version, _lengths, _crc, _header, _rest), do: {:error, :bad_version}

  defp config_of(<<>>), do: {:ok, nil}
  defp config_of(held), do: Config.decode(held)

  defp ensure(%module{} = disk, dir) do
    with {:ok, made} <- module.create_dir(disk, tmp_dir(dir)) do
      module.sync_dir(made, Path.dirname(dir))
    end
  end

  # A newer file on disk may never have been installed. The file just stored is always kept
  defp prune(%module{} = disk, dir, %SnapshotMeta{index: index, term: term}) do
    with {:ok, listed, names} <- module.list(disk, dir) do
      files = Enum.map(names, &parse/1)

      old =
        for {{_index, _term} = key, name} <- Enum.zip(files, names),
            key < {index, term},
            do: name

      delete_all(listed, dir, old)
    end
  end

  defp delete_all(disk, _dir, []), do: {:ok, disk}

  defp delete_all(%module{} = disk, dir, names) do
    deleted =
      Enum.reduce_while(names, {:ok, disk}, fn name, {:ok, current} ->
        case module.delete(current, Path.join(dir, name)) do
          {:ok, next} -> {:cont, {:ok, next}}
          {:error, _next, _reason} = failed -> {:halt, failed}
        end
      end)

    with {:ok, next} <- deleted, do: module.sync_dir(next, dir)
  end

  defp newest(disk, dir, names) do
    keys = for name <- names, key = parse(name), do: {key, name}
    sorted = keys |> Enum.sort(:desc) |> Enum.map(fn {_key, name} -> name end)

    first_whole(disk, dir, sorted)
  end

  defp first_whole(disk, _dir, []), do: {:ok, disk, nil}

  defp first_whole(%module{} = disk, dir, [name | rest]) do
    case module.read_file(disk, Path.join(dir, name)) do
      {:ok, read, bytes} -> whole_or_next(read, dir, name, bytes, rest)
      {:error, _disk, _reason} = failed -> failed
    end
  end

  defp whole_or_next(disk, dir, name, bytes, rest) do
    with {:ok, meta} <- meta(bytes),
         true <- name(meta.index, meta.term) == name do
      {:ok, disk, meta}
    else
      _bad -> first_whole(disk, dir, rest)
    end
  end

  defp parse(name) do
    case Regex.run(@name, name) do
      [_all, index, term] -> {String.to_integer(index), String.to_integer(term)}
      nil -> nil
    end
  end

  defp pad(number), do: number |> Integer.to_string() |> String.pad_leading(20, "0")
end
