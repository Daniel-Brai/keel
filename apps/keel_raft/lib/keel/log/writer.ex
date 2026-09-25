defmodule Keel.Log.Writer do
  @moduledoc "A failed write or sync is never retried so the owner restarts from what is durable"

  use GenServer

  alias Keel.Common.T
  alias Keel.Env.Disk
  alias Keel.Log.Recovery.Result
  alias Keel.Log.Store
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState

  require Logger

  @type request ::
          {:persist, T.persist_seq(), nil | HardState.t(), [Entry.t()]}
          | {:truncate_from, T.index()}
          | {:compact_log, T.index()}

  @type state :: %{
          store: nil | Store.t(),
          owner: pid() | atom(),
          pending: [request()],
          flush_scheduled?: boolean(),
          recovered: Result.t(),
          disk: Disk.t(),
          dir: Path.t(),
          store_opts: [Store.option()]
        }

  @type option ::
          {:disk, Disk.t()}
          | {:dir, Path.t()}
          | {:owner, pid() | atom()}
          | {:store, [Store.option()]}
          | {:name, atom()}
          | {:bootstrap, nil | Config.t()}
          | {:join, boolean()}

  @doc "The log opens in the writer process because a raw file handle works only there."
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name_opts = Keyword.take(opts, [:name])

    GenServer.start_link(__MODULE__, opts, name_opts)
  end

  @spec persist(GenServer.server(), T.persist_seq(), nil | HardState.t(), [Entry.t()]) :: :ok
  def persist(writer, seq, hard_state, entries),
    do: GenServer.cast(writer, {:persist, seq, hard_state, entries})

  @spec truncate_from(GenServer.server(), T.index()) :: :ok
  def truncate_from(writer, index), do: GenServer.cast(writer, {:truncate_from, index})

  @spec compact_log(GenServer.server(), T.index()) :: :ok
  def compact_log(writer, keep_from), do: GenServer.cast(writer, {:compact_log, keep_from})

  @spec recovered(GenServer.server()) :: Result.t()
  def recovered(writer), do: GenServer.call(writer, :recovered)

  @doc "A joining member has no log until its first message from a leader. This creates it."
  @spec open(GenServer.server()) :: :ok
  def open(writer), do: GenServer.call(writer, :open)

  @spec opened?(GenServer.server()) :: boolean()
  def opened?(writer), do: GenServer.call(writer, :opened?)

  @impl GenServer
  def init(opts) do
    disk = Keyword.fetch!(opts, :disk)
    dir = Keyword.fetch!(opts, :dir)

    state = %{
      store: nil,
      owner: Keyword.fetch!(opts, :owner),
      pending: [],
      flush_scheduled?: false,
      recovered: empty_result(),
      disk: disk,
      dir: dir,
      store_opts: Keyword.get(opts, :store, []),
      remover: spawn_link(&remove_loop/0)
    }

    with {:ok, fresh?} <- fresh(state),
         {:ok, started} <- start(state, fresh?, opts) do
      send(started.remover, {:free_trash, disk, dir})

      {:ok, started}
    else
      {:error, reason} -> {:stop, {:open_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call(:recovered, _from, %{recovered: result} = state), do: {:reply, result, state}

  def handle_call(:opened?, _from, state), do: {:reply, state.store != nil, state}

  def handle_call(:open, _from, %{store: store} = state) when store != nil,
    do: {:reply, :ok, state}

  def handle_call(:open, _from, state) do
    case open_store(state, nil) do
      {:ok, opened} -> {:reply, :ok, opened}
      {:error, reason} -> {:stop, {:open_failed, reason}, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_cast({:persist, _seq, _hard_state, _entries} = request, state),
    do: enqueue(state, request)

  def handle_cast({:truncate_from, _index} = request, state), do: enqueue(state, request)
  def handle_cast({:compact_log, _keep_from} = request, state), do: enqueue(state, request)

  @impl GenServer
  def handle_info(:flush, state) do
    batch = Enum.reverse(state.pending)
    operations = ops(batch)

    with {:ok, written} <- Store.write(state.store, operations),
         {:ok, synced} <- Store.sync(written) do
      notify(state.owner, batch)

      {doomed, taken} = Store.take_compacted(synced)
      remove_later(state, doomed)

      {:noreply, %{state | store: taken, pending: [], flush_scheduled?: false}}
    else
      {:error, _store, reason} ->
        Logger.error("log writer stopping after #{inspect(reason)}")
        {:stop, {:shutdown, reason}, state}
    end
  end

  defp fresh(state) do
    case Store.exists?(state.disk, state.dir) do
      {:ok, _disk, exists?} -> {:ok, not exists?}
      {:error, _disk, reason} -> {:error, reason}
    end
  end

  # A joining member opens its log when a leader first speaks to it
  defp start(state, true, opts) do
    case {Keyword.get(opts, :bootstrap), Keyword.get(opts, :join, false)} do
      {nil, true} -> {:ok, state}
      {config, _join} -> open_store(state, config)
    end
  end

  defp start(state, false, _opts), do: open_store(state, nil)

  defp open_store(state, config) do
    case Store.open(state.disk, state.dir, state.store_opts) do
      {:ok, store, result} -> found(state, store, result, config)
      {:error, _disk, reason} -> {:error, reason}
    end
  end

  defp found(state, store, result, nil), do: {:ok, %{state | store: store, recovered: result}}

  defp found(state, store, %Result{entries: []} = result, %Config{} = config) do
    entry = %Entry{
      term: 0,
      index: 1,
      kind: :config,
      data: Config.encode(config),
      time_ms: 0
    }

    case Store.bootstrap(store, result, entry) do
      {:ok, synced, bootstrapped} -> {:ok, %{state | store: synced, recovered: bootstrapped}}
      {:error, _disk, reason} -> {:error, reason}
    end
  end

  defp empty_result do
    %Result{
      hard_state: %HardState{term: 0, voted_for: nil, commit: 0},
      entries: [],
      last_segment: nil,
      next_seq: 1,
      tail: [],
      dropped: [],
      torn_tail?: false
    }
  end

  defp enqueue(%{flush_scheduled?: true} = state, request),
    do: {:noreply, %{state | pending: [request | state.pending]}}

  defp enqueue(state, request) do
    send(self(), :flush)

    {:noreply, %{state | pending: [request | state.pending], flush_scheduled?: true}}
  end

  # Only the newest hard state of a batch matters so it goes last
  defp ops(batch) do
    ops = Enum.flat_map(batch, &request_ops/1)
    {hard_states, others} = Enum.split_with(ops, &match?({:hard_state, _state}, &1))

    others ++ Enum.take(hard_states, -1)
  end

  defp request_ops({:truncate_from, index}), do: [{:truncate_from, index}]
  defp request_ops({:compact_log, keep_from}), do: [{:compact_log, keep_from}]

  defp request_ops({:persist, _seq, hard_state, entries}),
    do: [{:entries, entries} | hard_state_ops(hard_state)]

  defp hard_state_ops(nil), do: []
  defp hard_state_ops(%HardState{} = hard_state), do: [{:hard_state, hard_state}]

  defp notify(owner, batch) do
    seqs = for {:persist, seq, _hard_state, _entries} <- batch, do: seq

    newest = Enum.max(seqs, fn -> nil end)

    if newest, do: send(owner, {:persisted, newest}), else: :ok
  end

  defp remove_later(_state, []), do: :ok

  defp remove_later(state, paths),
    do: send(state.remover, {:remove, state.disk, state.dir, paths})

  # On some file systems freeing a large file makes every sync on the disk wait.
  # Old segments go to the trash first. Then each shrinks a little at a time beside the writer.
  @free_step_bytes 4_194_304
  @free_pause_ms 50

  defp remove_loop do
    receive do
      {:remove, disk, dir, paths} -> discarded(Store.discard(disk, dir, paths))
      {:free_trash, disk, dir} -> discarded(Store.trash(disk, dir))
    end
  end

  defp discarded({:ok, disk, trashed}), do: free_each(disk, trashed)
  defp discarded({:error, _disk, reason}), do: exit({:compact_failed, reason})

  defp free_each(_disk, []), do: remove_loop()

  defp free_each(disk, [path | rest]) do
    case free(disk, path) do
      {:ok, freed} -> free_each(freed, rest)
      {:error, _disk, reason} -> exit({:compact_failed, reason})
    end
  end

  defp free(%module{} = disk, path) do
    with {:ok, opened, handle} <- module.open(disk, path, [:write]),
         {:ok, sized, size} <- module.size(opened, handle),
         {:ok, cut} <- shrink(sized, handle, size),
         {:ok, closed} <- module.close(cut, handle) do
      module.delete(closed, path)
    end
  end

  defp shrink(disk, _handle, 0), do: {:ok, disk}

  defp shrink(%module{} = disk, handle, size) do
    target = max(size - @free_step_bytes, 0)

    case module.truncate(disk, handle, target) do
      {:ok, cut} -> paused(cut, handle, target)
      {:error, _disk, _reason} = failed -> failed
    end
  end

  defp paused(disk, handle, size) do
    Process.sleep(@free_pause_ms)

    shrink(disk, handle, size)
  end
end
