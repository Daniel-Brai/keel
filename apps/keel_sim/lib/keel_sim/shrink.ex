defmodule KeelSim.Shrink do
  @moduledoc "A replay that fails another way does not count as a smaller failure"

  alias KeelSim.Cluster
  alias KeelSim.Liveness

  @type class :: atom() | {:liveness, Liveness.kind()}

  @spec class(Cluster.result()) :: nil | class()
  def class(%{violations: [violation | _rest]}), do: elem(violation, 0)
  def class(%{liveness: [{:liveness, _kind} = liveness | _rest]}), do: liveness
  def class(%{}), do: nil

  @spec shrink(Cluster.opts(), [Cluster.action()], class()) :: [Cluster.action()]
  def shrink(opts, schedule, class) do
    heal? = match?({:liveness, _kind}, class)

    fails? = fn candidate ->
      result = KeelSim.replay(opts, candidate, heal?)

      class(result) == class
    end

    if fails?.(schedule), do: minimize(schedule, fails?, 2), else: schedule
  end

  defp minimize(schedule, _fails?, _chunks) when length(schedule) < 2, do: schedule

  defp minimize(schedule, fails?, chunks) do
    count = length(schedule)
    size = ceil(count / chunks)
    candidates = complements(schedule, size)

    case first_failing(candidates, fails?) do
      nil -> refine(schedule, fails?, chunks)
      shorter -> minimize(shorter, fails?, max(chunks - 1, 2))
    end
  end

  defp refine(schedule, fails?, chunks) do
    count = length(schedule)
    finer = min(chunks * 2, count)

    if chunks >= count,
      do: schedule,
      else: minimize(schedule, fails?, finer)
  end

  # Candidates run side by side. The first one in order wins. That keeps the result fixed.
  defp first_failing(candidates, fails?) do
    candidates
    |> Task.async_stream(fn candidate -> {candidate, fails?.(candidate)} end, timeout: :infinity)
    |> Enum.find_value(fn {:ok, {candidate, failing?}} -> if failing?, do: candidate end)
  end

  # Each candidate leaves out one chunk
  defp complements(schedule, size) do
    chunks = Enum.chunk_every(schedule, size)
    last = length(chunks) - 1

    for skipped <- 0..last do
      chunks |> List.delete_at(skipped) |> Enum.concat()
    end
  end
end
