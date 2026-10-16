defmodule KeelSim.Regression.EntryTimeNeverGoesBackTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # Each member reads its own skewed clock. The first leader stamps entries.
  # A second leader of a higher term then stamps more. Log time must never go back
  # whichever clock is ahead.
  @script [
    {:elect, "m1", ["m2"]},
    {:propose, "m1", "a"},
    {:tick, "m1", 3},
    {:propose, "m1", "b"},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:elect, "m2", ["m3"]},
    {:propose, "m2", "c"},
    {:tick, "m2", 3},
    {:propose, "m2", "d"},
    {:exchange, "m2", "m3"},
    {:exchange, "m2", "m1"},
    {:exchange, "m2", "m3"}
  ]

  describe "leaders with skewed clocks" do
    test "leave the invariants clean" do
      cluster = Scenario.play(@script, %{size: 3, skew_ms: 5_000})

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "keep log time from going back on every seed" do
      times = Enum.map(1..12, &Scenario.log_times(@script, %{size: 3, seed: &1, skew_ms: 5_000}))

      sorted = Enum.map(times, &Scenario.sorted?/1)
      expected = List.duplicate(true, 12)

      assert sorted == expected
    end

    test "stamp each entry with the time of a leader clock" do
      cluster = Scenario.play(@script, %{size: 3})

      leader = Scenario.node(cluster, "m2")

      assert leader.log.last_time_ms > 0
    end
  end
end
