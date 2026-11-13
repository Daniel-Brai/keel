defmodule Bench.Stats do
  @type summary :: %{
          count: non_neg_integer(),
          p50: number(),
          p90: number(),
          p99: number(),
          max: number()
        }

  @doc "The samples are microseconds. Percentiles take the nearest rank."
  @spec summary([number()]) :: summary()
  def summary([]), do: %{count: 0, p50: 0, p90: 0, p99: 0, max: 0}

  def summary(samples) do
    sorted = Enum.sort(samples)
    count = length(sorted)

    %{
      count: count,
      p50: percentile(sorted, count, 50),
      p90: percentile(sorted, count, 90),
      p99: percentile(sorted, count, 99),
      max: List.last(sorted)
    }
  end

  @spec ms(number()) :: String.t()
  def ms(micros) do
    millis = micros / 1_000

    :erlang.float_to_binary(millis, decimals: 2)
  end

  defp percentile(sorted, count, percent) do
    rank = max(ceil(count * percent / 100), 1)

    Enum.at(sorted, rank - 1)
  end
end
