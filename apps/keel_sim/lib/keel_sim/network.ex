defmodule KeelSim.Network do
  @moduledoc "Messages reorder only across links and never inside one"

  alias Keel.Common.T
  alias Keel.Raft.Message

  defstruct links: %{}, blocked: MapSet.new()

  @type link :: {T.member_id(), T.member_id()}

  @type t :: %__MODULE__{
          links: %{link() => :queue.queue(Message.t())},
          blocked: MapSet.t(link())
        }

  @spec enqueue(t(), Message.t()) :: t()
  def enqueue(%__MODULE__{} = network, %{from: from, to: to} = message) do
    if MapSet.member?(network.blocked, {from, to}) do
      network
    else
      queue = Map.get(network.links, {from, to}, :queue.new())
      %{network | links: Map.put(network.links, {from, to}, :queue.in(message, queue))}
    end
  end

  # Sorted so a pick never depends on map order
  @spec ready_links(t()) :: [link()]
  def ready_links(%__MODULE__{} = network) do
    network.links
    |> Enum.reject(fn {link, queue} ->
      :queue.is_empty(queue) or MapSet.member?(network.blocked, link)
    end)
    |> Enum.map(fn {link, _queue} -> link end)
    |> Enum.sort()
  end

  @spec take(t(), link()) :: {Message.t(), t()}
  def take(%__MODULE__{} = network, link) do
    {{:value, message}, rest} = network.links |> Map.fetch!(link) |> :queue.out()
    {message, %{network | links: Map.put(network.links, link, rest)}}
  end

  @spec duplicate(t(), link()) :: t()
  def duplicate(%__MODULE__{} = network, link) do
    {:value, message} = network.links |> Map.fetch!(link) |> :queue.peek()
    enqueue(network, message)
  end

  @spec isolate(t(), T.member_id(), [T.member_id()]) :: t()
  def isolate(%__MODULE__{} = network, id, ids) do
    cut = for other <- ids, other != id, link <- [{id, other}, {other, id}], do: link
    %{network | blocked: MapSet.union(network.blocked, MapSet.new(cut))}
  end

  @spec heal(t()) :: t()
  def heal(%__MODULE__{} = network), do: %{network | blocked: MapSet.new()}

  @spec blocked_members(t()) :: [T.member_id()]
  def blocked_members(%__MODULE__{blocked: blocked}) do
    blocked |> Enum.flat_map(fn {from, to} -> [from, to] end) |> Enum.uniq() |> Enum.sort()
  end

  @spec partitioned?(t()) :: boolean()
  def partitioned?(%__MODULE__{blocked: blocked}), do: MapSet.size(blocked) > 0
end
