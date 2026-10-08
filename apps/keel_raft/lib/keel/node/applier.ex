defmodule Keel.Node.Applier do
  use GenServer

  alias Keel.Common.Name
  alias Keel.Common.T
  alias Keel.Log.Snapshot
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.SnapshotMeta

  require Logger

  @fields ~w[module machine applied applied_term applied_time_ms config server disk snap_dir task]a
  @enforce_keys @fields
  defstruct @fields

  @type callers :: %{T.index() => GenServer.from()}

  @type t :: %__MODULE__{
          module: module(),
          machine: term(),
          applied: T.index(),
          applied_term: T.term_number(),
          applied_time_ms: non_neg_integer(),
          config: nil | Config.t(),
          server: GenServer.server(),
          disk: struct(),
          snap_dir: Path.t(),
          task: nil | reference()
        }

  @spec name(atom()) :: atom()
  def name(group), do: Name.build("keel", group, "_applier")

  def start_link(opts) do
    name_opts = Keyword.take(opts, [:name])

    GenServer.start_link(__MODULE__, opts, name_opts)
  end

  @spec apply_entries(GenServer.server(), [Entry.t()], callers()) :: :ok
  def apply_entries(applier, entries, callers),
    do: GenServer.cast(applier, {:apply, entries, callers})

  @spec query(GenServer.server(), term(), GenServer.from()) :: :ok
  def query(applier, query, from), do: GenServer.cast(applier, {:query, query, from})

  @spec take_snapshot(GenServer.server(), T.index()) :: :ok
  def take_snapshot(applier, index), do: GenServer.cast(applier, {:take_snapshot, index})

  @spec install_snapshot(GenServer.server(), SnapshotMeta.t()) :: :ok
  def install_snapshot(applier, meta), do: GenServer.cast(applier, {:install_snapshot, meta})

  @impl GenServer
  def init(opts) do
    module = Keyword.fetch!(opts, :machine)
    config = Keyword.get(opts, :machine_config, %{})

    state = %__MODULE__{
      module: module,
      machine: module.init(config),
      applied: 0,
      applied_term: 0,
      applied_time_ms: 0,
      config: nil,
      server: Keyword.fetch!(opts, :server),
      disk: Keyword.fetch!(opts, :disk),
      snap_dir: Keyword.fetch!(opts, :snap_dir),
      task: nil
    }

    restore_newest(state)
  end

  @impl GenServer
  def handle_cast({:apply, entries, callers}, %__MODULE__{} = state) do
    applied = Enum.reduce(entries, state, &apply_entry(&1, &2, callers))

    send(applied.server, {:applied, applied.applied})

    {:noreply, applied}
  end

  def handle_cast({:query, query, from}, %__MODULE__{} = state) do
    GenServer.reply(from, {:read, ask(state, query)})

    {:noreply, state}
  end

  def handle_cast({:take_snapshot, _index}, %__MODULE__{task: task} = state) when task != nil,
    do: {:noreply, state}

  def handle_cast({:take_snapshot, _index}, %__MODULE__{} = state) do
    parent = self()
    job = job(state)
    {_pid, ref} = spawn_monitor(fn -> send(parent, {:snapshot_written, write(job)}) end)

    {:noreply, %__MODULE__{state | task: ref}}
  end

  def handle_cast({:install_snapshot, meta}, %__MODULE__{} = state) do
    case load(state, meta) do
      {:ok, installed} ->
        send(installed.server, {:applied, meta.index})

        {:noreply, installed}

      {:error, reason} ->
        {:stop, {:shutdown, {:install_failed, reason}}, state}
    end
  end

  @impl GenServer
  def handle_info({:snapshot_written, {:ok, meta}}, %__MODULE__{} = state) do
    send(state.server, {:snapshot_taken, meta})

    {:noreply, settle(state)}
  end

  def handle_info({:snapshot_written, {:error, reason}}, %__MODULE__{} = state) do
    Logger.error("snapshot failed with #{inspect(reason)}")
    send(state.server, :snapshot_failed)

    {:noreply, settle(state)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %__MODULE__{task: ref} = state) do
    Logger.error("snapshot task stopped with #{inspect(reason)}")
    send(state.server, :snapshot_failed)

    {:noreply, %__MODULE__{state | task: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, %__MODULE__{} = state),
    do: {:noreply, state}

  defp apply_entry(%Entry{} = entry, %__MODULE__{} = state, callers) do
    {machine, result} = run(state, entry)

    case Map.fetch(callers, entry.index) do
      {:ok, from} -> GenServer.reply(from, {:applied, result})
      :error -> :ok
    end

    %__MODULE__{
      state
      | machine: machine,
        applied: entry.index,
        applied_term: entry.term,
        applied_time_ms: entry.time_ms,
        config: config_after(state.config, entry)
    }
  end

  defp config_after(_held, %Entry{kind: :config, data: data}), do: Config.decode!(data)
  defp config_after(held, %Entry{}), do: held

  defp ask(%__MODULE__{} = state, query) do
    state.module.query(state.machine, query)
  rescue
    _error -> {:error, :query_failed}
  end

  defp run(%__MODULE__{} = state, %Entry{} = entry) do
    state.module.apply(state.machine, entry)
  rescue
    _error -> exit({:shutdown, {:apply_crashed, entry.index}})
  end

  # The state value is immutable so the task holds a copy while apply goes on
  defp job(%__MODULE__{} = state) do
    %{
      module: state.module,
      machine: state.machine,
      index: state.applied,
      term: state.applied_term,
      time_ms: state.applied_time_ms,
      config: state.config,
      disk: state.disk,
      dir: state.snap_dir
    }
  end

  defp write(job) do
    payload = job.module.snapshot(job.machine)

    case Snapshot.write(job.disk, job.dir, job.index, job.term, job.time_ms, payload, job.config) do
      {:ok, _disk, meta} -> {:ok, meta}
      {:error, _disk, reason} -> {:error, reason}
    end
  end

  defp settle(%__MODULE__{task: ref} = state) do
    if ref, do: Process.demonitor(ref, [:flush])

    %__MODULE__{state | task: nil}
  end

  defp restore_newest(%__MODULE__{} = state) do
    case Snapshot.latest(state.disk, state.snap_dir) do
      {:ok, _disk, nil} ->
        {:ok, state}

      {:ok, _disk, meta} ->
        restored(load(state, meta))

      {:error, _disk, reason} ->
        {:stop, {:shutdown, {:snapshot_unreadable, reason}}}
    end
  end

  defp restored({:ok, state}), do: {:ok, state}
  defp restored({:error, reason}), do: {:stop, {:shutdown, {:snapshot_unreadable, reason}}}

  defp load(%__MODULE__{} = state, %SnapshotMeta{} = meta) do
    with {:ok, _disk, payload} <- read(state, meta),
         {:ok, machine} <- state.module.restore(payload) do
      loaded = %__MODULE__{
        state
        | machine: machine,
          applied: meta.index,
          applied_term: meta.term,
          applied_time_ms: meta.time_ms,
          config: meta.config
      }

      {:ok, loaded}
    end
  end

  defp read(%__MODULE__{} = state, meta) do
    case Snapshot.read(state.disk, state.snap_dir, meta) do
      {:ok, _disk, _payload} = loaded -> loaded
      {:error, _disk, reason} -> {:error, reason}
    end
  end
end
