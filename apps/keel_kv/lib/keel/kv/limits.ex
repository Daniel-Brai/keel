defmodule Keel.KV.Limits do
  @moduledoc "Limits live in the machine state so every member reads the same value at the same index"

  @ids %{
    max_key_bytes: 1,
    max_value_bytes: 2,
    max_data_bytes: 3,
    session_ttl_ms: 4,
    max_sessions: 5,
    session_max_pending: 6,
    session_sweep_interval: 7,
    max_range_limit: 8
  }

  @defaults %{
    max_key_bytes: 1_024,
    max_value_bytes: 1_048_576,
    max_data_bytes: 268_435_456,
    session_ttl_ms: 60_000,
    max_sessions: 10_000,
    session_max_pending: 16,
    session_sweep_interval: 128,
    max_range_limit: 1_000
  }

  @ranges %{
    max_key_bytes: 1..65_535,
    max_value_bytes: 0..16_777_216,
    max_data_bytes: 1..1_099_511_627_776,
    session_ttl_ms: 1..31_536_000_000,
    max_sessions: 1..1_000_000,
    session_max_pending: 1..1_000,
    session_sweep_interval: 1..1_000_000,
    max_range_limit: 1..10_000
  }

  @type name ::
          :max_key_bytes
          | :max_value_bytes
          | :max_data_bytes
          | :session_ttl_ms
          | :max_sessions
          | :session_max_pending
          | :session_sweep_interval
          | :max_range_limit

  @type t :: %{name() => non_neg_integer()}

  @spec defaults() :: t()
  def defaults, do: @defaults

  @spec new(map()) :: t()
  def new(overrides), do: Map.merge(@defaults, Map.take(overrides, Map.keys(@defaults)))

  @spec id(name()) :: pos_integer()
  def id(name), do: Map.fetch!(@ids, name)

  @spec name(non_neg_integer()) :: {:ok, name()} | :error
  def name(id) do
    found = Enum.find(@ids, fn {_name, known} -> known == id end)

    case found do
      {name, _id} -> {:ok, name}
      nil -> :error
    end
  end

  @spec valid?(t()) :: boolean()
  def valid?(limits), do: Enum.all?(limits, fn {name, value} -> in_range?(name, value) end)

  @spec in_range?(atom(), term()) :: boolean()
  def in_range?(name, value) when is_integer(value) do
    case Map.fetch(@ranges, name) do
      {:ok, range} -> value in range
      :error -> false
    end
  end

  def in_range?(_name, _value), do: false

  @spec to_pairs(t()) :: [{pos_integer(), non_neg_integer()}]
  def to_pairs(limits) do
    limits
    |> Enum.map(fn {name, value} -> {id(name), value} end)
    |> Enum.sort()
  end
end
