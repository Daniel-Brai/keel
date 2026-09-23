defmodule Keel.Log.Store do
  @moduledoc "A failed write or sync poisons the store so it refuses all later calls"

  alias Keel.Common.T
  alias Keel.Env.Disk
  alias Keel.Log.Codec
  alias Keel.Log.Codec.Header
  alias Keel.Log.Recovery
  alias Keel.Log.Recovery.Result
  alias Keel.Log.Snapshot
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState

  @fields ~w[
    dir
    disk
    config
    segment
    handle
    capacity
    offset
    next_seq
    synced
    commit
    dirty?
    poisoned
    closed
    current_max
    doomed
    hard_state
  ]a
  @enforce_keys @fields
  defstruct @fields

  @type config :: %{
          segment_bytes: pos_integer(),
          max_record_bytes: pos_integer(),
          cluster_id: <<_::128>>,
          member_tag: <<_::64>>,
          snap_dir: Path.t()
        }

  @type op ::
          {:hard_state, HardState.t()}
          | {:entries, [Entry.t()]}
          | {:truncate_from, T.index()}
          | {:compact_log, T.index()}

  @type reason :: term()

  @type t :: %__MODULE__{
          dir: Path.t(),
          disk: Disk.t(),
          config: config(),
          segment: pos_integer(),
          handle: Disk.handle(),
          capacity: pos_integer(),
          offset: non_neg_integer(),
          next_seq: pos_integer(),
          synced: {pos_integer(), non_neg_integer(), non_neg_integer()},
          commit: T.index(),
          dirty?: boolean(),
          poisoned: nil | reason(),
          closed: [{pos_integer(), T.index()}],
          current_max: T.index(),
          doomed: [{pos_integer(), T.index()}],
          hard_state: HardState.t()
        }

  @type option ::
          {:segment_bytes, pos_integer()}
          | {:max_record_bytes, pos_integer()}
          | {:cluster_id, <<_::128>>}
          | {:member, String.t()}
          | {:snap_dir, Path.t()}

  @doc "Every record that no sync mark proves durable is copied into a fresh segment first."
  @spec open(Disk.t(), Path.t(), [option()]) ::
          {:ok, t(), Result.t()} | {:error, Disk.t(), reason()}
  def open(disk, dir, opts) do
    config = config(opts, dir)

    with {:ok, disk, names} <- list_or_create(disk, dir),
         {:ok, disk, segments} <- read_all(disk, dir, names),
         {:ok, disk} <- Snapshot.clean_tmp(disk, config.snap_dir),
         {:ok, disk, snapshot} <- Snapshot.latest(disk, config.snap_dir),
         {:ok, result} <- recover(disk, segments, config, snapshot),
         {:ok, disk} <- drop_junk(disk, dir, result.dropped),
         {:ok, store} <- relog(disk, dir, config, result),
         {:ok, sealed} <- seal_discard(store, result) do
      {:ok, sealed, result}
    end
  end

  @doc "A directory with a segment already carries the identity of its member."
  @spec exists?(Disk.t(), Path.t()) :: {:ok, Disk.t(), boolean()} | {:error, Disk.t(), reason()}
  def exists?(disk, dir) do
    with {:ok, listed, names} <- list_if_present(disk, dir) do
      {:ok, listed, Enum.any?(names, &segment?/1)}
    end
  end

  @doc "A new member starts from one entry. It is synced before the member starts."
  @spec bootstrap(t(), Result.t(), Entry.t()) ::
          {:ok, t(), Result.t()} | {:error, Disk.t(), reason()}
  def bootstrap(%__MODULE__{} = store, %Result{} = result, %Entry{} = entry) do
    hard_state = %HardState{term: 0, voted_for: nil, commit: 0}

    with {:ok, written} <- write(store, [{:entries, [entry]}, {:hard_state, hard_state}]),
         {:ok, synced} <- sync(written) do
      {:ok, synced, %{result | entries: [entry]}}
    else
      {:error, failed, reason} -> {:error, failed.disk, reason}
    end
  end

  @spec read_log(Disk.t(), Path.t(), keyword()) ::
          {:ok, Disk.t(), Result.t()} | {:error, Disk.t(), reason()}
  def read_log(disk, dir, opts) do
    config = config(opts, dir)

    with {:ok, disk, names} <- list_if_present(disk, dir),
         {:ok, disk, segments} <- read_all(disk, dir, names),
         {:ok, disk, snapshot} <- Snapshot.latest(disk, config.snap_dir),
         {:ok, result} <- recover(disk, segments, config, snapshot) do
      {:ok, disk, result}
    end
  end

  @spec write(t(), [op()]) :: {:ok, t()} | {:error, t(), reason()}
  def write(%__MODULE__{poisoned: nil} = store, requested) do
    ops = with_hard_state(store, requested)

    with :ok <- check_ops(store, ops),
         bodies = bodies(store, ops),
         {:ok, store} <- write_bodies(store, bodies) do
      {:ok, track(store, ops)}
    else
      {:error, reason} -> {:error, poison(store, reason), reason}
      {:error, _store, _reason} = failed -> failed
    end
  end

  def write(%__MODULE__{poisoned: reason} = store, _ops), do: {:error, store, {:poisoned, reason}}

  @spec sync(t()) :: {:ok, t()} | {:error, t(), reason()}
  def sync(%__MODULE__{poisoned: nil, dirty?: false} = store), do: {:ok, store}

  def sync(%__MODULE__{poisoned: nil, disk: %module{} = disk} = store) do
    case module.sync(disk, store.handle) do
      {:ok, synced} ->
        mark = {store.segment, store.offset, store.next_seq - 1}

        {:ok, %{store | disk: synced, dirty?: false, synced: mark}}

      {:error, failed, reason} ->
        reason = {:sync_failed, reason}
        poisoned = poison(%{store | disk: failed}, reason)

        {:error, poisoned, reason}
    end
  end

  def sync(%__MODULE__{poisoned: reason} = store), do: {:error, store, {:poisoned, reason}}

  @spec disk(t()) :: Disk.t()
  def disk(%__MODULE__{disk: disk}), do: disk

  @spec dirty?(t()) :: boolean()
  def dirty?(%__MODULE__{dirty?: dirty?}), do: dirty?

  @spec snap_dir(t()) :: Path.t()
  def snap_dir(%__MODULE__{config: config}), do: config.snap_dir

  @spec segment_name(pos_integer()) :: String.t()
  def segment_name(number) do
    padded = number |> Integer.to_string() |> String.pad_leading(8, "0")

    padded <> ".seg"
  end

  defp config(opts, dir) do
    member = Keyword.get(opts, :member, "member")
    hash = :crypto.hash(:sha256, member)
    snap_dir = Keyword.get_lazy(opts, :snap_dir, fn -> Path.join(Path.dirname(dir), "snap") end)

    %{
      segment_bytes: Keyword.get(opts, :segment_bytes, 67_108_864),
      max_record_bytes: Keyword.get(opts, :max_record_bytes, 16_777_216),
      cluster_id: Keyword.get(opts, :cluster_id, <<0::128>>),
      member_tag: binary_part(hash, 0, 8),
      snap_dir: snap_dir
    }
  end

  defp expect(config), do: Map.take(config, ~w[cluster_id member_tag max_record_bytes]a)

  defp recover(disk, segments, config, snapshot) do
    case Recovery.recover(segments, expect(config), snapshot) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, disk, reason}
    end
  end

  defp list_or_create(%module{} = disk, dir) do
    case module.list(disk, dir) do
      {:error, missing, :enoent} -> create_dir(missing, dir)
      other -> other
    end
  end

  defp create_dir(%module{} = disk, dir) do
    parent = Path.dirname(dir)

    with {:ok, made} <- module.create_dir(disk, dir),
         {:ok, synced} <- module.sync_dir(made, parent) do
      {:ok, synced, []}
    end
  end

  defp list_if_present(%module{} = disk, dir) do
    case module.list(disk, dir) do
      {:error, missing, :enoent} -> {:ok, missing, []}
      other -> other
    end
  end

  defp read_all(disk, dir, names) do
    names
    |> Enum.filter(&segment?/1)
    |> read_each(disk, dir, [])
  end

  defp segment?(name), do: String.match?(name, ~r/\A\d{8}\.seg\z/)

  defp read_each([], disk, _dir, acc), do: {:ok, disk, Enum.reverse(acc)}

  defp read_each([name | rest], %module{} = disk, dir, acc) do
    path = Path.join(dir, name)

    case module.read_file(disk, path) do
      {:ok, read, binary} ->
        number = name |> binary_part(0, 8) |> String.to_integer()

        read_each(rest, read, dir, [{number, binary} | acc])

      {:error, _read, _reason} = failed ->
        failed
    end
  end

  defp drop_junk(disk, _dir, []), do: {:ok, disk}

  defp drop_junk(%module{} = disk, dir, numbers) do
    deleted =
      Enum.reduce_while(numbers, {:ok, disk}, fn number, {:ok, current} ->
        name = segment_name(number)
        path = Path.join(dir, name)

        case module.delete(current, path) do
          {:ok, next} -> {:cont, {:ok, next}}
          {:error, _next, _reason} = failed -> {:halt, failed}
        end
      end)

    with {:ok, next} <- deleted, do: module.sync_dir(next, dir)
  end

  # Entries that recovery dropped stay in old segments so the drop is written down before anything else
  defp seal_discard(store, %Result{first_index: first, keep_from: first}), do: {:ok, store}

  defp seal_discard(store, %Result{first_index: first}) do
    with {:ok, written} <- write(store, [{:truncate_from, first}, {:compact_log, first}]),
         {:ok, synced} <- sync(written),
         {:ok, removed} <- remove_compacted(synced) do
      {:ok, removed}
    else
      {:error, failed, reason} -> {:error, failed.disk, reason}
    end
  end

  defp relog(disk, dir, config, %Result{} = result) do
    number = next_number(result)
    first_seq = first_seq(result)
    initial = tail_bytes(result, number)
    offset = Codec.header_len() + byte_size(initial)
    capacity = max(config.segment_bytes, offset)

    spec = %{number: number, first_seq: first_seq, capacity: capacity, initial: initial}

    with {:ok, disk, handle} <- create_segment(disk, dir, config, spec) do
      store = %__MODULE__{
        dir: dir,
        disk: disk,
        config: config,
        segment: number,
        handle: handle,
        capacity: capacity,
        offset: offset,
        next_seq: result.next_seq,
        synced: {number, offset, result.next_seq - 1},
        commit: result.hard_state.commit,
        dirty?: false,
        poisoned: nil,
        closed: result.segments,
        current_max: tail_max(result),
        doomed: [],
        hard_state: result.hard_state
      }

      {:ok, store}
    end
  end

  defp next_number(%Result{dropped: [number]}), do: number
  defp next_number(%Result{last_segment: nil}), do: 1
  defp next_number(%Result{last_segment: last}), do: last + 1

  defp tail_max(%Result{tail: tail}) do
    indexes = for {_seq, {:entries, entries}} <- tail, entry <- entries, do: entry.index

    Enum.max(indexes, fn -> 0 end)
  end

  defp first_seq(%Result{tail: [{seq, _body} | _rest]}), do: seq
  defp first_seq(%Result{next_seq: next}), do: next

  defp tail_bytes(%Result{tail: tail}, number) do
    tail
    |> Enum.map(fn {seq, body} -> Codec.encode_record(body, seq, number) end)
    |> :erlang.list_to_binary()
  end

  # The file is synced before its directory so a durable name never points at a bad header
  defp create_segment(%module{} = disk, dir, config, spec) do
    path = Path.join(dir, segment_name(spec.number))

    header =
      Codec.encode_header(%Header{
        segment: spec.number,
        cluster_id: config.cluster_id,
        member_tag: config.member_tag,
        first_seq: spec.first_seq
      })

    with {:ok, opened, handle} <- module.open(disk, path, ~w[write create exclusive]a),
         {:ok, written} <- module.write_at(opened, handle, 0, [header, spec.initial]),
         {:ok, reserved} <- module.reserve(written, handle, 0, spec.capacity),
         {:ok, synced} <- module.sync(reserved, handle),
         {:ok, durable} <- module.sync_dir(synced, dir) do
      {:ok, durable, handle}
    end
  end

  defp check_ops(%__MODULE__{} = store, ops) do
    cond do
      Enum.any?(ops, &committed_truncate?(store, &1)) -> {:error, :truncate_committed}
      Enum.any?(ops, &too_large?(store, &1)) -> {:error, :entry_too_large}
      true -> :ok
    end
  end

  defp committed_truncate?(store, {:truncate_from, index}), do: index <= store.commit
  defp committed_truncate?(_store, _op), do: false

  defp too_large?(store, {:entries, entries}),
    do: Enum.any?(entries, &(entry_size(&1) + 4 > store.config.max_record_bytes))

  defp too_large?(_store, _op), do: false

  defp entry_size(%Entry{} = entry), do: Entry.bytes(entry)

  defp bodies(store, ops), do: Enum.flat_map(ops, &op_bodies(store, &1))

  defp op_bodies(_store, {:hard_state, hard_state}), do: [{:hard_state, hard_state}]
  defp op_bodies(_store, {:truncate_from, index}), do: [{:truncate, index}]
  defp op_bodies(_store, {:compact_log, keep_from}), do: [{:compacted, keep_from}]
  defp op_bodies(_store, {:entries, []}), do: []

  defp op_bodies(store, {:entries, entries}),
    do: entries |> chunk(store.config.max_record_bytes - 4) |> Enum.map(&{:entries, &1})

  defp chunk(entries, limit) do
    {chunks, current, _size} =
      Enum.reduce(entries, {[], [], 0}, fn entry, {chunks, current, size} ->
        added = entry_size(entry)

        if current != [] and size + added > limit,
          do: {[Enum.reverse(current) | chunks], [entry], added},
          else: {chunks, [entry | current], size + added}
      end)

    Enum.reverse([Enum.reverse(current) | chunks])
  end

  defp write_bodies(store, []), do: {:ok, store}

  defp write_bodies(store, bodies) do
    batch = encode_batch(store, bodies)
    size = byte_size(batch)

    if store.offset + size > store.capacity,
      do: rotate_then_write(store, bodies, size),
      else: write_batch(store, bodies, batch)
  end

  defp rotate_then_write(store, bodies, size) do
    with {:ok, rotated} <- rotate(store, size) do
      write_bodies(rotated, bodies)
    end
  end

  defp write_batch(%__MODULE__{disk: %module{} = disk} = store, bodies, batch) do
    case module.write_at(disk, store.handle, store.offset, batch) do
      {:ok, written} ->
        written_store = %{
          store
          | disk: written,
            offset: store.offset + byte_size(batch),
            next_seq: store.next_seq + length(bodies) + 1,
            dirty?: true,
            current_max: entries_max(bodies, store.current_max)
        }

        {:ok, written_store}

      {:error, failed, reason} ->
        reason = {:write_failed, reason}
        poisoned = poison(%{store | disk: failed}, reason)

        {:error, poisoned, reason}
    end
  end

  # The old segment is synced first so the new one never starts after a lost gap
  defp rotate(%__MODULE__{} = store, size) do
    with {:ok, synced} <- sync(store),
         {:ok, disk} <- close_handle(synced),
         spec = next_spec(synced, size),
         {:ok, created, handle} <- create_segment(disk, store.dir, store.config, spec) do
      offset = Codec.header_len()

      rotated = %{
        synced
        | disk: created,
          segment: spec.number,
          handle: handle,
          capacity: spec.capacity,
          offset: offset,
          synced: {spec.number, offset, synced.next_seq - 1},
          closed: Enum.concat(synced.closed, [{synced.segment, synced.current_max}]),
          current_max: 0
      }

      {:ok, rotated}
    else
      {:error, failed, reason} when is_struct(failed, __MODULE__) -> {:error, failed, reason}
      {:error, failed, reason} -> fail_rotation(store, failed, reason)
    end
  end

  defp close_handle(%__MODULE__{disk: %module{} = disk, handle: handle}),
    do: module.close(disk, handle)

  defp next_spec(store, size) do
    used = Codec.header_len() + size

    %{
      number: store.segment + 1,
      first_seq: store.next_seq,
      capacity: max(store.config.segment_bytes, used),
      initial: <<>>
    }
  end

  defp fail_rotation(store, disk, reason) do
    reason = {:write_failed, {:rotate, reason}}
    poisoned = poison(%{store | disk: disk}, reason)

    {:error, poisoned, reason}
  end

  defp encode_batch(store, bodies) do
    {segment, offset, seq} = store.synced
    marked = [{:sync_mark, segment, offset, seq} | bodies]

    marked
    |> Enum.with_index(store.next_seq)
    |> Enum.map(fn {body, seq} -> Codec.encode_record(body, seq, store.segment) end)
    |> :erlang.list_to_binary()
  end

  # Segments hold the only copy of an old hard state so each compaction writes it again first
  defp with_hard_state(%__MODULE__{} = store, ops) do
    compacting? = Enum.any?(ops, &match?({:compact_log, _keep}, &1))
    stated? = Enum.any?(ops, &match?({:hard_state, _state}, &1))

    if compacting? and not stated?,
      do: Enum.concat(ops, [{:hard_state, store.hard_state}]),
      else: ops
  end

  defp track(store, ops) do
    commits = for {:hard_state, %HardState{commit: commit}} <- ops, do: commit
    states = for {:hard_state, %HardState{} = state} <- ops, do: state
    keeps = for {:compact_log, keep_from} <- ops, do: keep_from
    keep = Enum.max(keeps, fn -> 0 end)

    # Only segments closed before the compaction record go. A later one may hold that record
    {doomed, remaining} = Enum.split_while(store.closed, fn {_number, max} -> max < keep end)

    %{
      store
      | commit: Enum.max([store.commit | commits]),
        hard_state: List.last(states, store.hard_state),
        closed: remaining,
        doomed: Enum.concat(store.doomed, doomed)
    }
  end

  defp entries_max(bodies, current) do
    indexes = for {:entries, entries} <- bodies, entry <- entries, do: entry.index

    Enum.max([current | indexes])
  end

  @doc "Paths of the segments below a durable compaction, oldest first. Deleting them can take long so the caller chooses when."
  @spec take_compacted(t()) :: {[Path.t()], t()}
  def take_compacted(%__MODULE__{dirty?: true} = store), do: {[], store}

  def take_compacted(%__MODULE__{} = store) do
    paths =
      Enum.map(store.doomed, fn {number, _max} -> Path.join(store.dir, segment_name(number)) end)

    {paths, %{store | doomed: []}}
  end

  @doc "Moves segments into the trash folder oldest first. Recovery never reads it so they can be freed slowly."
  @spec discard(Disk.t(), Path.t(), [Path.t()]) :: Disk.result([Path.t()])
  def discard(disk, _dir, []), do: {:ok, disk, []}

  def discard(%module{} = disk, dir, paths) do
    trash = trash_dir(dir)

    with {:ok, made} <- module.create_dir(disk, trash),
         {:ok, moved} <- move_all(made, trash, paths),
         {:ok, synced} <- module.sync_dir(moved, dir) do
      trashed = Enum.map(paths, &Path.join(trash, Path.basename(&1)))

      {:ok, synced, trashed}
    end
  end

  @doc "Files a crash left in the trash folder."
  @spec trash(Disk.t(), Path.t()) :: Disk.result([Path.t()])
  def trash(%module{} = disk, dir) do
    trash = trash_dir(dir)

    case module.list(disk, trash) do
      {:ok, listed, names} -> {:ok, listed, Enum.map(names, &Path.join(trash, &1))}
      {:error, listed, :enoent} -> {:ok, listed, []}
      {:error, _listed, _reason} = failed -> failed
    end
  end

  @spec remove_compacted(t()) :: {:ok, t()} | {:error, t(), reason()}
  @doc "Discards and deletes the compacted segments at once. The writer frees them slowly instead."
  def remove_compacted(%__MODULE__{} = store) do
    {paths, taken} = take_compacted(store)

    with {:ok, discarded, trashed} <- discard(taken.disk, taken.dir, paths),
         {:ok, deleted} <- delete_all(discarded, trashed) do
      {:ok, %{taken | disk: deleted}}
    else
      {:error, failed, reason} -> fail_compaction(%{taken | disk: failed}, reason)
    end
  end

  defp trash_dir(dir), do: Path.join(dir, "trash")

  defp move_all(disk, trash, paths) do
    Enum.reduce_while(paths, {:ok, disk}, fn path, {:ok, %module{} = current} ->
      target = Path.join(trash, Path.basename(path))

      case module.rename(current, path, target) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _next, _reason} = failed -> {:halt, failed}
      end
    end)
  end

  defp delete_all(disk, paths) do
    Enum.reduce_while(paths, {:ok, disk}, fn path, {:ok, %module{} = current} ->
      case module.delete(current, path) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _next, _reason} = failed -> {:halt, failed}
      end
    end)
  end

  defp fail_compaction(store, reason) do
    reason = {:compact_failed, reason}

    {:error, poison(store, reason), reason}
  end

  defp poison(store, reason), do: %{store | poisoned: reason}
end
