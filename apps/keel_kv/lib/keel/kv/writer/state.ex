defmodule Keel.KV.Writer.State do
  alias Keel.KV.Writer.Call

  @fields ~w[
    group
    nodes
    route
    target
    client_id
    next_seq
    outstanding
    calls
    reqids
    counter
    heard_ms
    attempt_timeout_ms
    backoff_ms
    backoff_max_ms
    max_redirects
  ]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          group: atom(),
          nodes: nil | [node()],
          route: (node() -> GenServer.server()),
          target: nil | node(),
          client_id: nil | pos_integer(),
          next_seq: pos_integer(),
          outstanding: :gb_sets.set(pos_integer()),
          calls: %{Call.id() => Call.t()},
          reqids: :gen_server.request_id_collection(),
          counter: non_neg_integer(),
          heard_ms: nil | integer(),
          attempt_timeout_ms: pos_integer(),
          backoff_ms: pos_integer(),
          backoff_max_ms: pos_integer(),
          max_redirects: non_neg_integer()
        }

  @spec candidates(t()) :: [node()]
  def candidates(%__MODULE__{nodes: nil}), do: [node() | Node.list()]
  def candidates(%__MODULE__{nodes: nodes}), do: nodes

  @spec target(t()) :: node()
  def target(%__MODULE__{target: nil} = state), do: state |> candidates() |> hd()
  def target(%__MODULE__{target: target}), do: target

  @spec rotate(t()) :: t()
  def rotate(%__MODULE__{} = state) do
    nodes = candidates(state)
    current = target(state)
    found = Enum.find_index(nodes, &(&1 == current))
    position = position_or_before_start(found)
    next = Enum.at(nodes, rem(position + 1, length(nodes)))

    %__MODULE__{state | target: next}
  end

  defp position_or_before_start(nil), do: -1
  defp position_or_before_start(index), do: index

  @spec backoff(t(), pos_integer()) :: pos_integer()
  def backoff(%__MODULE__{} = state, attempt) do
    wait = min(state.backoff_ms * attempt, state.backoff_max_ms)
    half = div(wait, 2)

    half + :rand.uniform(half + 1)
  end

  @spec put_call(t(), Call.t()) :: t()
  def put_call(%__MODULE__{} = state, %Call{} = call),
    do: %__MODULE__{state | calls: Map.put(state.calls, call.id, call)}

  @spec queued(t()) :: [Call.t()]
  def queued(%__MODULE__{calls: calls}) do
    for {id, %Call{status: :queued} = call} <- calls, id != :register, do: call
  end

  @spec sent(t()) :: [Call.t()]
  def sent(%__MODULE__{calls: calls}) do
    for {id, %Call{seq: seq} = call} <- calls, id != :register, seq != nil, do: call
  end
end
