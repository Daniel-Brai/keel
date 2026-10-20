defmodule Keel.Node.SnapshotReceiver do
  @moduledoc "The server hears of a snapshot only after its file and directory are synced"

  use GenServer

  alias Keel.Common.Name
  alias Keel.Env.Disk
  alias Keel.Log.Snapshot
  alias Keel.Raft.SnapshotMeta

  require Logger

  @type option ::
          {:disk, Disk.t()}
          | {:snap_dir, Path.t()}
          | {:server, GenServer.server()}
          | {:name, atom()}

  @spec name(atom()) :: atom()
  def name(group), do: Name.build("keel", group, "_snapshot_receiver")

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name_opts = Keyword.take(opts, [:name])

    GenServer.start_link(__MODULE__, opts, name_opts)
  end

  @spec chunk(GenServer.server(), SnapshotMeta.t(), non_neg_integer(), binary(), boolean()) :: :ok
  def chunk(receiver, meta, offset, data, done?),
    do: GenServer.cast(receiver, {:chunk, meta, offset, data, done?})

  @impl GenServer
  def init(opts) do
    disk = Keyword.fetch!(opts, :disk)
    dir = Keyword.fetch!(opts, :snap_dir)
    server = Keyword.fetch!(opts, :server)

    case Snapshot.clean_tmp(disk, dir) do
      {:ok, cleaned} -> {:ok, %{disk: cleaned, dir: dir, server: server, current: nil}}
      {:error, _disk, reason} -> {:stop, {:shutdown, {:snapshot_dir_unusable, reason}}}
    end
  end

  @impl GenServer
  def handle_cast({:chunk, meta, 0, data, done?}, state) do
    restarted = discard(state)

    with {:ok, started} <- open_part(restarted, meta),
         {:ok, written} <- write(started, 0, data) do
      {:noreply, finish_if(written, done?)}
    else
      {:error, failed, reason} -> {:noreply, failed(failed, reason)}
    end
  end

  def handle_cast({:chunk, meta, offset, data, done?}, %{current: %{meta: meta}} = state) do
    case write(state, offset, data) do
      {:ok, written} -> {:noreply, finish_if(written, done?)}
      {:error, failed, reason} -> {:noreply, failed(failed, reason)}
    end
  end

  def handle_cast({:chunk, _meta, _offset, _data, _done?}, state), do: {:noreply, state}

  defp open_part(%{disk: %module{} = disk} = state, meta) do
    part = Snapshot.part_path(state.dir, meta)
    tmp = Snapshot.tmp_dir(state.dir)

    with {:ok, made} <- module.create_dir(disk, tmp),
         {:ok, opened, handle} <- module.open(made, part, [:write, :create]) do
      current = %{meta: meta, part: part, handle: handle, written: 0}

      {:ok, %{state | disk: opened, current: current}}
    else
      {:error, failed, reason} -> {:error, %{state | disk: failed}, reason}
    end
  end

  defp write(%{disk: %module{} = disk, current: current} = state, offset, data) do
    case module.write_at(disk, current.handle, offset, data) do
      {:ok, written} ->
        moved = %{current | written: offset + byte_size(data)}

        {:ok, %{state | disk: written, current: moved}}

      {:error, failed, reason} ->
        {:error, %{state | disk: failed}, reason}
    end
  end

  defp finish_if(state, false), do: state

  defp finish_if(%{current: current} = state, true) do
    case complete(state, current) do
      {:ok, installed} ->
        send(installed.server, {:snapshot_received, current.meta})

        %{installed | current: nil}

      {:error, failed, reason} ->
        failed(failed, reason)
    end
  end

  # The file is checked after its bytes are synced and before it gets its final name
  defp complete(%{disk: %module{} = disk} = state, current) do
    with {:ok, synced} <- module.sync(disk, current.handle),
         {:ok, closed} <- module.close(synced, current.handle) do
      closed_state = %{state | disk: closed, current: %{current | handle: nil}}

      install(closed_state, current)
    else
      {:error, failed, reason} -> {:error, %{state | disk: failed}, reason}
    end
  end

  defp install(%{disk: %module{} = disk} = state, current) do
    with {:ok, read, bytes} <- module.read_file(disk, current.part),
         :ok <- check(bytes, current.meta),
         {:ok, installed} <- Snapshot.install(read, state.dir, current.meta) do
      {:ok, %{state | disk: installed}}
    else
      {:error, reason} -> {:error, state, reason}
      {:error, failed, reason} -> {:error, %{state | disk: failed}, reason}
    end
  end

  defp check(bytes, meta) do
    case Snapshot.verify(bytes, meta) do
      :ok -> :ok
      {:error, reason} -> {:error, {:bad_snapshot, reason}}
    end
  end

  defp failed(state, reason) do
    Logger.warning("snapshot receive failed with #{inspect(reason)}")

    discard(state)
  end

  defp discard(%{current: nil} = state), do: state

  defp discard(%{current: %{handle: nil} = current} = state) do
    deleted = delete(state.disk, current.part)

    %{state | disk: deleted, current: nil}
  end

  defp discard(%{disk: %module{} = disk, current: current} = state) do
    {:ok, closed} = module.close(disk, current.handle)
    deleted = delete(closed, current.part)

    %{state | disk: deleted, current: nil}
  end

  defp delete(%module{} = disk, part) do
    case module.delete(disk, part) do
      {:ok, next} -> next
      {:error, next, _reason} -> next
    end
  end
end
