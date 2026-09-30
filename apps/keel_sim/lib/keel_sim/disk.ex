defmodule KeelSim.Disk do
  @behaviour Keel.Env.Disk

  @fields ~w[files dirs handles next_handle faults profile rng]a
  @enforce_keys @fields
  defstruct @fields

  @type profile :: :strict | :prefix | :subset | :torn

  @type fault :: :fail_next_sync | :fail_next_write

  @type write :: {non_neg_integer(), binary()}

  @type file :: %{durable: binary(), pending: [write()]}

  @type dir_op :: {:add | :remove, String.t()}

  @type dir :: %{durable: MapSet.t(String.t()), pending: [dir_op()]}

  @type t :: %__MODULE__{
          files: %{Path.t() => file()},
          dirs: %{Path.t() => dir()},
          handles: %{pos_integer() => Path.t()},
          next_handle: pos_integer(),
          faults: MapSet.t(fault()),
          profile: profile(),
          rng: :rand.state()
        }

  @profiles ~w[strict prefix subset torn]a

  @spec profiles() :: [profile()]
  def profiles, do: @profiles

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    profile = Keyword.get(opts, :profile, :strict)
    seed = Keyword.get(opts, :seed, {1, 2, 3})
    rng = :rand.seed_s(:exsss, seed)

    %__MODULE__{
      files: %{},
      dirs: %{},
      handles: %{},
      next_handle: 1,
      faults: MapSet.new(),
      profile: profile,
      rng: rng
    }
  end

  @spec inject(t(), fault()) :: t()
  def inject(%__MODULE__{} = disk, fault), do: %{disk | faults: MapSet.put(disk.faults, fault)}

  @spec crash(t()) :: t()
  def crash(%__MODULE__{} = disk) do
    {dirs, dir_rng} =
      disk.dirs
      |> Enum.sort()
      |> Enum.map_reduce(disk.rng, &crash_dir(&1, &2, disk.profile))

    live = live_paths(dirs)
    surviving = disk.files |> Enum.sort() |> Enum.filter(fn {path, _file} -> path in live end)
    {files, rng} = Enum.map_reduce(surviving, dir_rng, &crash_file(&1, &2, disk.profile))

    %{
      disk
      | files: Map.new(files),
        dirs: Map.new(dirs),
        handles: %{},
        faults: MapSet.new(),
        rng: rng
    }
  end

  @impl Keel.Env.Disk
  def open(%__MODULE__{} = disk, path, modes) do
    exists? = exists?(disk, path)

    cond do
      exists? and :exclusive in modes -> {:error, disk, :eexist}
      exists? -> grant(disk, path)
      :create in modes -> create(disk, path)
      true -> {:error, disk, :enoent}
    end
  end

  @impl Keel.Env.Disk
  def close(%__MODULE__{} = disk, handle),
    do: {:ok, %{disk | handles: Map.delete(disk.handles, handle)}}

  @impl Keel.Env.Disk
  def write_at(%__MODULE__{} = disk, handle, offset, data) do
    if MapSet.member?(disk.faults, :fail_next_write) do
      {:error, %{disk | faults: MapSet.delete(disk.faults, :fail_next_write)}, :eio}
    else
      write = {offset, :erlang.iolist_to_binary(data)}

      add_write(disk, handle, write)
    end
  end

  @impl Keel.Env.Disk
  def read_at(%__MODULE__{} = disk, handle, offset, length) do
    case fetch_handle(disk, handle) do
      {:ok, path} -> read_path(disk, path, offset, length)
      :error -> {:error, disk, :ebadf}
    end
  end

  @impl Keel.Env.Disk
  def read_file(%__MODULE__{} = disk, path) do
    if exists?(disk, path),
      do: {:ok, disk, view(disk.files[path])},
      else: {:error, disk, :enoent}
  end

  @impl Keel.Env.Disk
  def sync(%__MODULE__{} = disk, handle) do
    case fetch_handle(disk, handle) do
      :error -> {:error, disk, :ebadf}
      {:ok, _path} -> settle_file(disk, handle)
    end
  end

  @impl Keel.Env.Disk
  def reserve(%__MODULE__{} = disk, handle, offset, length) do
    case fetch_handle(disk, handle) do
      {:ok, path} -> grow(disk, handle, file_size(disk, path), offset + length)
      :error -> {:error, disk, :ebadf}
    end
  end

  # Only trash files are cut. Nothing reads them again so the cut is durable at once
  @impl Keel.Env.Disk
  def truncate(%__MODULE__{} = disk, handle, length) do
    case fetch_handle(disk, handle) do
      {:ok, path} -> {:ok, cut_file(disk, path, length)}
      :error -> {:error, disk, :ebadf}
    end
  end

  @impl Keel.Env.Disk
  def rename(%__MODULE__{} = disk, from, to) do
    if exists?(disk, from) and Map.has_key?(disk.dirs, Path.dirname(to)),
      do: {:ok, move(disk, from, to)},
      else: {:error, disk, :enoent}
  end

  @impl Keel.Env.Disk
  def delete(%__MODULE__{} = disk, path) do
    if exists?(disk, path),
      do: {:ok, queue_name(disk, path, :remove)},
      else: {:error, disk, :enoent}
  end

  @impl Keel.Env.Disk
  def list(%__MODULE__{} = disk, dir) do
    case Map.fetch(disk.dirs, dir) do
      {:ok, record} -> {:ok, disk, record |> names() |> Enum.sort()}
      :error -> {:error, disk, :enoent}
    end
  end

  @impl Keel.Env.Disk
  def create_dir(%__MODULE__{} = disk, path) do
    paths = ancestry(path)
    missing = Enum.reject(paths, &Map.has_key?(disk.dirs, &1))
    added = Map.new(missing, &{&1, %{durable: MapSet.new(), pending: []}})

    {:ok, %{disk | dirs: Map.merge(disk.dirs, added)}}
  end

  @impl Keel.Env.Disk
  def sync_dir(%__MODULE__{} = disk, dir) do
    case Map.fetch(disk.dirs, dir) do
      {:ok, record} ->
        settled = %{durable: names(record), pending: []}

        {:ok, %{disk | dirs: Map.put(disk.dirs, dir, settled)}}

      :error ->
        {:error, disk, :enoent}
    end
  end

  @impl Keel.Env.Disk
  def size(%__MODULE__{} = disk, handle) do
    case fetch_handle(disk, handle) do
      {:ok, path} -> {:ok, disk, file_size(disk, path)}
      :error -> {:error, disk, :ebadf}
    end
  end

  defp file_size(disk, path) do
    bytes = view(disk.files[path])

    byte_size(bytes)
  end

  defp read_path(disk, path, offset, length) do
    bytes = view(disk.files[path])
    size = byte_size(bytes)
    count = min(length, size - offset)

    if offset >= size, do: :eof, else: {:ok, disk, binary_part(bytes, offset, count)}
  end

  defp ancestry(path) do
    parent = Path.dirname(path)

    if parent == path, do: [path], else: [path | ancestry(parent)]
  end

  defp exists?(disk, path) do
    dir = Path.dirname(path)
    name = Path.basename(path)

    case Map.fetch(disk.dirs, dir) do
      {:ok, record} -> name in names(record)
      :error -> false
    end
  end

  defp names(%{durable: durable, pending: pending}) do
    pending
    |> Enum.reverse()
    |> Enum.reduce(durable, fn
      {:add, name}, set -> MapSet.put(set, name)
      {:remove, name}, set -> MapSet.delete(set, name)
    end)
  end

  defp create(disk, path) do
    if Map.has_key?(disk.dirs, Path.dirname(path)) do
      file = %{durable: <<>>, pending: []}
      fresh = %{disk | files: Map.put(disk.files, path, file)}

      grant(queue_name(fresh, path, :add), path)
    else
      {:error, disk, :enoent}
    end
  end

  defp queue_name(disk, path, kind) do
    dir = Path.dirname(path)
    record = Map.fetch!(disk.dirs, dir)
    name = Path.basename(path)
    queued = %{record | pending: [{kind, name} | record.pending]}

    %{disk | dirs: Map.put(disk.dirs, dir, queued)}
  end

  defp grant(disk, path) do
    handle = disk.next_handle
    opened = %{disk | handles: Map.put(disk.handles, handle, path), next_handle: handle + 1}

    {:ok, opened, handle}
  end

  defp fetch_handle(disk, handle), do: Map.fetch(disk.handles, handle)

  defp add_write(disk, handle, write) do
    case fetch_handle(disk, handle) do
      {:ok, path} ->
        file = disk.files[path]
        written = %{file | pending: [write | file.pending]}

        {:ok, %{disk | files: Map.put(disk.files, path, written)}}

      :error ->
        {:error, disk, :ebadf}
    end
  end

  defp grow(disk, _handle, size, target) when target <= size, do: {:ok, disk}

  defp grow(disk, handle, size, target) do
    zeros = :binary.copy(<<0>>, target - size)

    add_write(disk, handle, {size, zeros})
  end

  defp cut_file(disk, path, length) do
    bytes = view(disk.files[path])
    size = min(length, byte_size(bytes))
    kept = binary_part(bytes, 0, size)
    file = %{durable: kept, pending: []}

    %{disk | files: Map.put(disk.files, path, file)}
  end

  defp settle_file(disk, handle) do
    if MapSet.member?(disk.faults, :fail_next_sync) do
      {:error, %{disk | faults: MapSet.delete(disk.faults, :fail_next_sync)}, :eio}
    else
      path = Map.fetch!(disk.handles, handle)
      file = disk.files[path]
      settled = %{durable: view(file), pending: []}

      {:ok, %{disk | files: Map.put(disk.files, path, settled)}}
    end
  end

  defp move(disk, from, to) do
    {file, rest} = Map.pop!(disk.files, from)
    moved = %{disk | files: Map.put(rest, to, file)}

    moved |> queue_name(from, :remove) |> queue_name(to, :add)
  end

  defp view(%{durable: durable, pending: pending}),
    do: pending |> Enum.reverse() |> Enum.reduce(durable, &overlay/2)

  defp overlay({offset, data}, bytes) do
    size = byte_size(bytes)

    if offset >= size,
      do: append_at(bytes, offset - size, data),
      else: splice(bytes, offset, data)
  end

  defp append_at(bytes, gap, data), do: <<bytes::binary, 0::size(gap * 8), data::binary>>

  defp splice(bytes, offset, data) do
    size = byte_size(bytes)
    after_write = offset + byte_size(data)
    head = binary_part(bytes, 0, offset)

    tail =
      if after_write < size, do: binary_part(bytes, after_write, size - after_write), else: <<>>

    <<head::binary, data::binary, tail::binary>>
  end

  defp live_paths(dirs) do
    for {dir, %{durable: names}} <- dirs,
        name <- names,
        into: MapSet.new(),
        do: Path.join(dir, name)
  end

  defp crash_dir({dir, record}, rng, profile) do
    written = Enum.reverse(record.pending)
    {kept, next} = surviving_prefix(written, rng, profile)
    pending = Enum.reverse(kept)
    durable = names(%{durable: record.durable, pending: pending})

    {{dir, %{durable: durable, pending: []}}, next}
  end

  defp crash_file({path, file}, rng, profile) do
    written = Enum.reverse(file.pending)
    {survivors, after_survivors} = surviving_writes(written, rng, profile)
    {writes, next} = tear(survivors, after_survivors, profile)
    durable = Enum.reduce(writes, file.durable, &overlay/2)

    {{path, %{durable: durable, pending: []}}, next}
  end

  defp surviving_prefix([], rng, _profile), do: {[], rng}
  defp surviving_prefix(_pending, rng, :strict), do: {[], rng}

  defp surviving_prefix(pending, rng, _profile) do
    {count, next} = :rand.uniform_s(length(pending) + 1, rng)

    {Enum.take(pending, count - 1), next}
  end

  defp surviving_writes(pending, rng, profile) when profile in [:strict, :prefix],
    do: surviving_prefix(pending, rng, profile)

  defp surviving_writes(pending, rng, _profile) do
    Enum.flat_map_reduce(pending, rng, fn write, state ->
      {keep, next} = :rand.uniform_s(2, state)

      kept = kept_write(keep, write)

      {kept, next}
    end)
  end

  defp kept_write(1, write), do: [write]
  defp kept_write(_draw, _write), do: []

  defp tear([], rng, _profile), do: {[], rng}

  defp tear(writes, rng, :torn) do
    [{offset, data} | earlier] = Enum.reverse(writes)
    size = byte_size(data)
    {cut, after_cut} = :rand.uniform_s(size, rng)
    {garbage, next} = :rand.bytes_s(size - cut + 1, after_cut)
    kept = binary_part(data, 0, cut - 1)
    torn = <<kept::binary, garbage::binary>>

    {Enum.reverse([{offset, torn} | earlier]), next}
  end

  defp tear(writes, rng, _profile), do: {writes, rng}
end
