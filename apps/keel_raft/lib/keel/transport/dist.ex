defmodule Keel.Transport.Dist do
  @behaviour Keel.Transport

  alias Keel.Transport.Dist.Peer
  alias Keel.Transport.Dist.Supervisor, as: DistSupervisor

  @impl Keel.Transport
  def child_spec(opts) do
    %{
      id: __MODULE__,
      type: :supervisor,
      start: {DistSupervisor, :start_link, [opts]}
    }
  end

  @impl Keel.Transport
  def send(group, to, message) do
    registry = DistSupervisor.registry(group)

    with [{peer, _value}] <- Registry.lookup(registry, to) do
      GenServer.cast(peer, {:send, message})
    end

    :ok
  end

  @impl Keel.Transport
  def update_members(group, members) do
    Enum.each(members, fn {member_id, node} -> ensure_peer(group, member_id, node) end)

    registry = DistSupervisor.registry(group)
    held = Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
    gone = held -- Map.keys(members)

    Enum.each(gone, &stop_peer(group, &1))
  end

  defp stop_peer(group, member_id) do
    registry = DistSupervisor.registry(group)
    supervisor = DistSupervisor.peers(group)

    with [{peer, _value}] <- Registry.lookup(registry, member_id) do
      DynamicSupervisor.terminate_child(supervisor, peer)
    end

    :ok
  end

  defp ensure_peer(group, member_id, node) do
    registry = DistSupervisor.registry(group)

    case Registry.lookup(registry, member_id) do
      [{_peer, _value}] -> :ok
      [] -> start_peer(group, member_id, node)
    end
  end

  defp start_peer(group, member_id, node) do
    settings = DistSupervisor.settings(group)
    opts = Keyword.merge(settings, group: group, member_id: member_id, node: node)
    supervisor = DistSupervisor.peers(group)

    {:ok, _peer} = DynamicSupervisor.start_child(supervisor, {Peer, opts})

    :ok
  end
end
