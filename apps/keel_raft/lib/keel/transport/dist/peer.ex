defmodule Keel.Transport.Dist.Peer do
  use GenServer

  alias Keel.Transport.Dist.Supervisor, as: DistSupervisor
  alias Keel.Transport.Envelope

  @min_backoff_ms 100
  @max_backoff_ms 5_000
  @default_max_queue 4_096
  @default_check_every 64

  @fields ~w[
    member_id
    node
    cluster_id
    destination
    max_queue
    check_every
    connected?
    backoff_ms
    sent
    queued
  ]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          member_id: String.t(),
          node: node(),
          cluster_id: binary(),
          destination: atom(),
          max_queue: pos_integer(),
          check_every: pos_integer(),
          connected?: boolean(),
          backoff_ms: pos_integer(),
          sent: non_neg_integer(),
          queued: non_neg_integer()
        }

  def start_link(opts) do
    group = Keyword.fetch!(opts, :group)
    member_id = Keyword.fetch!(opts, :member_id)
    name = {:via, Registry, {DistSupervisor.registry(group), member_id}}

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(opts) do
    peer = %__MODULE__{
      member_id: Keyword.fetch!(opts, :member_id),
      node: Keyword.fetch!(opts, :node),
      cluster_id: Keyword.fetch!(opts, :cluster_id),
      destination: Keyword.fetch!(opts, :destination),
      max_queue: Keyword.get(opts, :max_queue, @default_max_queue),
      check_every: Keyword.get(opts, :check_every, @default_check_every),
      connected?: false,
      backoff_ms: @min_backoff_ms,
      sent: 0,
      queued: 0
    }

    send(self(), :connect)

    {:ok, peer}
  end

  @impl GenServer
  def handle_cast({:send, message}, %__MODULE__{} = peer) do
    counted = %__MODULE__{peer | sent: peer.sent + 1}
    checked = check_queue(counted)

    {:noreply, deliver(checked, message)}
  end

  @impl GenServer
  def handle_info(:connect, %__MODULE__{} = peer), do: {:noreply, connect(peer)}

  def handle_info({:nodedown, node}, %__MODULE__{node: node} = peer) do
    down = %__MODULE__{peer | connected?: false}

    Process.send_after(self(), :connect, down.backoff_ms)

    {:noreply, down}
  end

  def handle_info(_message, %__MODULE__{} = peer), do: {:noreply, peer}

  defp connect(%__MODULE__{} = peer) do
    if Node.connect(peer.node) do
      Node.monitor(peer.node, true)

      %__MODULE__{peer | connected?: true, backoff_ms: @min_backoff_ms}
    else
      Process.send_after(self(), :connect, peer.backoff_ms)

      %__MODULE__{peer | backoff_ms: min(peer.backoff_ms * 2, @max_backoff_ms)}
    end
  end

  # Reading the mailbox length costs a call so it runs once per few messages
  defp check_queue(%__MODULE__{sent: sent, check_every: every} = peer)
       when rem(sent, every) == 0 do
    {:message_queue_len, length} = Process.info(self(), :message_queue_len)

    %__MODULE__{peer | queued: length}
  end

  defp check_queue(%__MODULE__{} = peer), do: peer

  defp deliver(%__MODULE__{connected?: false} = peer, _message), do: peer

  defp deliver(%__MODULE__{queued: queued, max_queue: max} = peer, _message) when queued >= max,
    do: peer

  defp deliver(%__MODULE__{} = peer, message) do
    envelope = Envelope.wrap(peer.cluster_id, message)
    target = {peer.destination, peer.node}

    case :erlang.send(target, envelope, [:noconnect]) do
      :ok -> peer
      :noconnect -> reconnect(peer)
    end
  end

  defp reconnect(%__MODULE__{} = peer) do
    Process.send_after(self(), :connect, peer.backoff_ms)

    %__MODULE__{peer | connected?: false}
  end
end
