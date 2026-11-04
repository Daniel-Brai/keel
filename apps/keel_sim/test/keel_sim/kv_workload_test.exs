defmodule KeelSim.KvWorkloadTest do
  use ExUnit.Case, async: true

  alias KeelSim.History
  alias KeelSim.TestUtils

  describe "run/1 with bugs" do
    test "catches a dropped put" do
      opts = %{seed: 9, steps: 3_000, workload: :kv, bug: :kv_drop_put}

      result = KeelSim.run(opts)

      assert {:not_linearizable, _key} =
               hd(for {:not_linearizable, _} = found <- result.violations, do: found)
    end

    test "catches a retry that applies twice" do
      opts = %{seed: 61, steps: 3_000, workload: :kv, bug: :kv_no_dedupe}

      result = KeelSim.run(opts)

      assert {:not_linearizable, _key} =
               hd(for {:not_linearizable, _} = found <- result.violations, do: found)
    end

    test "leaves the same seed clean without the bug" do
      opts = %{seed: 45, steps: 3_000, workload: :kv}

      result = KeelSim.run(opts)

      assert result.violations == []
    end
  end

  describe "run/1" do
    test "keeps every check on key value seeds" do
      results = TestUtils.swept(1..60, %{steps: 2_000, workload: :kv})

      found = TestUtils.checked_violations(results)

      assert found == []
    end

    test "leaves the cluster live after the heal" do
      seeds = TestUtils.liveness_failures(1..40, %{steps: 2_000, workload: :kv})

      assert seeds == []
    end

    test "gives the same history for the same seed" do
      opts = %{seed: 9, size: 3, steps: 2_000, workload: :kv}

      first = KeelSim.run(opts)
      second = KeelSim.run(opts)

      assert first.history == second.history
    end

    test "gives the same trace for the same seed" do
      opts = %{seed: 21, steps: 2_000, workload: :kv}

      first = KeelSim.run(opts)
      second = KeelSim.run(opts)

      assert first.trace_hash == second.trace_hash
    end

    test "ends every operation it invoked" do
      result = KeelSim.run(%{seed: 21, steps: 2_000, workload: :kv})

      open = TestUtils.unmatched_invokes(result.history)

      assert open == []
    end

    test "records ok and failed and unknown endings" do
      results = TestUtils.swept(1..60, %{steps: 2_000, workload: :kv})

      kinds = TestUtils.event_kinds(results)

      assert kinds == ~w[fail info invoke ok]a
    end

    test "records no history for the chain workload" do
      result = KeelSim.run(%{seed: 5, steps: 500, workload: :chain})

      assert result.history == []
    end

    test "records every operation with its own process while it waits" do
      result = KeelSim.run(%{seed: 21, steps: 2_000, workload: :kv})

      ops = History.ops(result.history)
      overlapping = TestUtils.self_overlaps(ops)

      assert overlapping == []
    end

    test "replays a schedule to the same history" do
      result = KeelSim.run(%{seed: 21, steps: 1_000, workload: :kv})

      replayed = KeelSim.replay(result.config, result.schedule)

      assert replayed.history == result.history
    end
  end
end
