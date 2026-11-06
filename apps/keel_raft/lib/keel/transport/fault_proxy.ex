defmodule Keel.Transport.FaultProxy do
  @moduledoc "Breaks messages on purpose so it must never run in production"

  @behaviour Keel.Transport

  alias Keel.Transport.FaultProxy.Control

  @type group :: Keel.Transport.group()

  @type rule ::
          :block
          | {:drop, float()}
          | {:duplicate, float()}
          | {:delay, non_neg_integer(), non_neg_integer()}

  @kinds ~w[block drop duplicate delay]a

  @impl Keel.Transport
  def child_spec(opts) do
    group = Keyword.fetch!(opts, :group)
    inner = Keyword.get(opts, :inner, Keel.Transport.Dist)
    inner_spec = inner.child_spec(opts)
    control_spec = {Control, group: group, inner: inner}
    children = [inner_spec, control_spec]

    %{
      id: __MODULE__,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_all]]}
    }
  end

  @impl Keel.Transport
  def send(group, to, message) do
    table = Control.table(group)
    inner = :ets.lookup_element(table, :inner, 2)

    case decide(table, to) do
      :drop -> :ok
      {:deliver, copies, nil} -> deliver(inner, group, to, message, copies)
      {:deliver, copies, ms} -> delay(group, to, message, copies, ms)
    end
  end

  @impl Keel.Transport
  def update_members(group, members) do
    table = Control.table(group)
    inner = :ets.lookup_element(table, :inner, 2)

    inner.update_members(group, members)
  end

  @doc "A rule of the same kind is replaced."
  @spec set(group(), String.t(), rule()) :: :ok
  def set(group, to, rule) do
    table = Control.table(group)
    kind = kind(rule)

    true = :ets.insert(table, {{:rule, to, kind}, rule})

    :ok
  end

  @spec block(group(), String.t()) :: :ok
  def block(group, to), do: set(group, to, :block)

  @spec unblock(group(), String.t()) :: :ok
  def unblock(group, to), do: clear(group, to, :block)

  @spec clear(group(), String.t(), :block | :drop | :duplicate | :delay) :: :ok
  def clear(group, to, kind) do
    table = Control.table(group)

    true = :ets.delete(table, {:rule, to, kind})

    :ok
  end

  @spec clear(group()) :: :ok
  def clear(group) do
    table = Control.table(group)

    true = :ets.match_delete(table, {{:rule, :_, :_}, :_})

    :ok
  end

  @spec partition(group(), [String.t()]) :: :ok
  def partition(group, blocked), do: Enum.each(blocked, &block(group, &1))

  defp kind(:block), do: :block
  defp kind({kind, _first}) when kind in @kinds, do: kind
  defp kind({:delay, _min, _max}), do: :delay

  defp decide(table, to) do
    rules =
      for kind <- @kinds, [{_key, rule}] <- [:ets.lookup(table, {:rule, to, kind})], do: rule

    Enum.reduce(rules, {:deliver, 1, nil}, &apply_rule/2)
  end

  defp apply_rule(_rule, :drop), do: :drop
  defp apply_rule(:block, _decision), do: :drop

  defp apply_rule({:drop, probability}, decision) do
    if :rand.uniform() < probability, do: :drop, else: decision
  end

  defp apply_rule({:duplicate, probability}, {:deliver, copies, delay}) do
    if :rand.uniform() < probability,
      do: {:deliver, copies + 1, delay},
      else: {:deliver, copies, delay}
  end

  defp apply_rule({:delay, min, max}, {:deliver, copies, _delay}) do
    {:deliver, copies, min + :rand.uniform(max - min + 1) - 1}
  end

  defp deliver(inner, group, to, message, copies) do
    Enum.each(1..copies, fn _copy -> inner.send(group, to, message) end)
  end

  defp delay(group, to, message, copies, ms), do: Control.delay(group, to, message, copies, ms)
end
