defmodule Keel.Node.Waiters do
  alias Keel.Common.T

  @type t :: %{reference() => GenServer.from()}

  @type callers :: %{T.index() => GenServer.from()}

  @spec new() :: t()
  def new, do: %{}

  @spec size(t()) :: non_neg_integer()
  def size(waiters), do: map_size(waiters)

  @spec add(t(), reference(), GenServer.from()) :: t()
  def add(waiters, ref, from), do: Map.put(waiters, ref, from)

  @spec take(t(), %{T.index() => reference()}) :: {callers(), t()}
  def take(waiters, refs) do
    Enum.reduce(refs, {%{}, waiters}, fn {index, ref}, {callers, rest} ->
      case Map.pop(rest, ref) do
        {nil, _rest} -> {callers, rest}
        {from, left} -> {Map.put(callers, index, from), left}
      end
    end)
  end

  @spec pop(t(), reference()) :: {nil | GenServer.from(), t()}
  def pop(waiters, ref), do: Map.pop(waiters, ref)
end
