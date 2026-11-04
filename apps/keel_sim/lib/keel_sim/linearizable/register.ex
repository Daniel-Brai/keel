defmodule KeelSim.Linearizable.Register do
  @moduledoc "A revision of `{:open, floor}` means a write of unknown revision took effect above the floor"

  alias KeelSim.Client.Op

  @type revision :: non_neg_integer() | {:open, non_neg_integer()}

  @type state :: {nil | binary(), revision()}

  def init, do: {nil, 0}

  @spec step(state(), Op.t()) :: {:ok, state()} | :invalid
  def step({_value, rev}, %Op{kind: :put, status: :ok, value: value, result: {:ok, new}}) do
    if new > above(rev), do: {:ok, {value, new}}, else: :invalid
  end

  def step({_value, rev}, %Op{kind: :put, status: :info, value: value}),
    do: {:ok, {value, {:open, above(rev)}}}

  def step({nil, _floor} = state, %Op{kind: :delete, status: :ok, result: {:ok, 0}}),
    do: {:ok, state}

  def step({value, rev}, %Op{kind: :delete, status: :ok, result: {:ok, new}})
      when value != nil and new > 0 do
    if new > above(rev), do: {:ok, {nil, new}}, else: :invalid
  end

  def step({_value, rev}, %Op{kind: :delete, status: :info}), do: {:ok, {nil, above(rev)}}

  def step({nil, _floor} = state, %Op{kind: :get, status: :ok, result: {:ok, nil}}),
    do: {:ok, state}

  def step({value, rev}, %Op{kind: :get, status: :ok, result: {:ok, {value, seen}}})
      when value != nil do
    if settles?(rev, seen), do: {:ok, {value, seen}}, else: :invalid
  end

  def step(state, %Op{kind: :compare_and_set, status: :ok, result: {:ok, new}} = op),
    do: swap(state, op, new)

  def step(state, %Op{kind: :compare_and_set, status: :ok, result: {:error, _reason}} = op),
    do: mismatch(state, op)

  def step(state, %Op{kind: :compare_and_set, status: :info, expected: expected, value: value}) do
    case hold(state, expected) do
      {:ok, {_value, rev}} -> {:ok, {value, {:open, above(rev)}}}
      :invalid -> :invalid
    end
  end

  def step(_state, %Op{}), do: :invalid

  defp swap(state, %Op{expected: expected, value: value}, new) do
    with {:ok, {_value, rev}} <- hold(state, expected),
         true <- new > above(rev) do
      {:ok, {value, new}}
    else
      _other -> :invalid
    end
  end

  defp mismatch(state, %Op{expected: expected, result: {:error, {:revision_mismatch, current}}})
       when current != expected,
       do: hold(state, current)

  defp mismatch(_state, %Op{}), do: :invalid

  # The state when the current revision is the given one. Revision 0 means no value
  defp hold({nil, _floor} = state, 0), do: {:ok, state}
  defp hold({_value, _rev}, 0), do: :invalid
  defp hold({nil, _floor}, _revision), do: :invalid

  defp hold({value, rev}, revision) do
    if settles?(rev, revision), do: {:ok, {value, revision}}, else: :invalid
  end

  defp settles?(rev, seen) when is_integer(rev), do: rev == seen
  defp settles?({:open, floor}, seen), do: seen > floor

  defp above({:open, floor}), do: floor
  defp above(rev), do: rev
end
