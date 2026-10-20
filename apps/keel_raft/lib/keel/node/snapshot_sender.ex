defmodule Keel.Node.SnapshotSender do
  @moduledoc "Sends the chunks of one snapshot file and never decides that it arrived"

  use GenServer, restart: :temporary

  alias Keel.Common.T
  alias Keel.Env.Disk
  alias Keel.Log.Snapshot
  alias Keel.Raft.SnapshotMeta
  alias Keel.Raft.SnapshotStream

  @type option ::
          {:disk, Disk.t()}
          | {:snap_dir, Path.t()}
          | {:meta, SnapshotMeta.t()}
          | {:term, T.term_number()}
          | {:seq, pos_integer()}
          | {:from, T.member_id()}
          | {:to, T.member_id()}
          | {:group, atom()}
          | {:transport, module()}
          | {:chunk_bytes, pos_integer()}
          | {:max_inflight, pos_integer()}
          | {:ack_timeout_ms, pos_integer()}

  @spec ack(GenServer.server(), non_neg_integer()) :: :ok
  def ack(sender, offset), do: GenServer.cast(sender, {:ack, offset})

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    %module{} = disk = Keyword.fetch!(opts, :disk)
    meta = Keyword.fetch!(opts, :meta)
    snap_dir = Keyword.fetch!(opts, :snap_dir)
    path = Snapshot.path(snap_dir, meta)
    stream_keys = ~w[meta term seq from to chunk_bytes max_inflight]a

    case module.open(disk, path, [:read]) do
      {:ok, opened, handle} ->
        state = %{
          disk: opened,
          handle: handle,
          stream: opts |> Keyword.take(stream_keys) |> SnapshotStream.new(),
          group: Keyword.fetch!(opts, :group),
          transport: Keyword.fetch!(opts, :transport),
          ack_timeout_ms: Keyword.fetch!(opts, :ack_timeout_ms),
          timer: nil
        }

        {:ok, state, {:continue, :pump}}

      {:error, _disk, reason} ->
        {:stop, {:shutdown, {:snapshot_unreadable, reason}}}
    end
  end

  @impl GenServer
  def handle_continue(:pump, state), do: pump(state)

  @impl GenServer
  def handle_cast({:ack, offset}, state) do
    acked = %{state | stream: SnapshotStream.ack(state.stream, offset)}

    pump(acked)
  end

  @impl GenServer
  def handle_info({:stalled, timer}, %{timer: timer} = state) do
    restarted = %{state | stream: SnapshotStream.restart(state.stream)}

    pump(restarted)
  end

  def handle_info({:stalled, _old}, state), do: {:noreply, state}

  defp pump(%{stream: stream} = state) do
    {pulled, ranges} = SnapshotStream.pull(stream)

    case send_ranges(%{state | stream: pulled}, ranges) do
      {:ok, sent} -> {:noreply, arm(sent)}
      {:error, reason} -> {:stop, {:shutdown, {:snapshot_read_failed, reason}}, state}
    end
  end

  defp send_ranges(state, []), do: {:ok, state}

  defp send_ranges(%{disk: %module{} = disk} = state, [{offset, length} | rest]) do
    case read(module, disk, state.handle, offset, length) do
      {:ok, next, data} ->
        message = SnapshotStream.message(state.stream, offset, data)
        :ok = state.transport.send(state.group, state.stream.to, message)

        send_ranges(%{state | disk: next}, rest)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read(_module, disk, _handle, _offset, 0), do: {:ok, disk, <<>>}

  defp read(module, disk, handle, offset, length) do
    case module.read_at(disk, handle, offset, length) do
      {:ok, next, data} -> {:ok, next, data}
      :eof -> {:error, :eof}
      {:error, _next, reason} -> {:error, reason}
    end
  end

  defp arm(state) do
    if SnapshotStream.complete?(state.stream) do
      %{state | timer: nil}
    else
      timer = make_ref()
      Process.send_after(self(), {:stalled, timer}, state.ack_timeout_ms)

      %{state | timer: timer}
    end
  end
end
