defmodule KeelSim.SweepTest do
  use ExUnit.Case, async: true

  alias KeelSim.Coverage
  alias KeelSim.TestUtils

  @moduletag :sweep
  @moduletag timeout: :infinity

  describe "a swarm sweep" do
    test "keeps every safety and liveness check" do
      opts = %{steps: 5_000}

      seeds = TestUtils.failing_seeds(1..500, opts)

      assert seeds == []
    end

    test "reaches every named path" do
      coverage = TestUtils.coverage_after(1..500, %{steps: 5_000})

      missing = Coverage.missing(coverage)

      assert missing == []
    end

    test "keeps one leader per term in five member clusters" do
      opts = %{size: 5, steps: 5_000}

      seeds = TestUtils.failing_seeds(501..800, opts)

      assert seeds == []
    end

    test "keeps every acknowledged entry on a torn disk" do
      opts = %{size: 3, steps: 5_000, crash_profile: :torn}

      seeds = TestUtils.failing_seeds(801..1_000, opts)

      assert seeds == []
    end
  end

  describe "a key value sweep" do
    test "keeps every history linearizable" do
      opts = %{steps: 5_000, workload: :kv}

      results = TestUtils.swept(1..500, opts)
      found = TestUtils.checked_violations(results)

      assert found == []
    end

    test "keeps every liveness check" do
      seeds = TestUtils.liveness_failures(501..800, %{steps: 5_000, workload: :kv})

      assert seeds == []
    end

    test "keeps every history linearizable on five members" do
      opts = %{size: 5, steps: 5_000, workload: :kv}

      results = TestUtils.swept(801..1_000, opts)
      found = TestUtils.checked_violations(results)

      assert found == []
    end

    test "keeps every history linearizable with a large clock skew" do
      opts = %{size: 3, steps: 5_000, workload: :kv, skew_ms: 20_000}

      results = TestUtils.swept(1_001..1_200, opts)
      found = TestUtils.checked_violations(results)

      assert found == []
    end

    test "never gives up on a history" do
      opts = %{steps: 5_000, workload: :kv, check_max_states: 1_000_000}

      results = TestUtils.swept(1..300, opts)
      kinds = results |> Enum.flat_map(& &1.violations) |> Enum.map(&elem(&1, 0))

      assert :linearizability_unknown not in kinds
    end
  end

  describe "membership" do
    test "keeps every safety and liveness check while members come and go" do
      opts = %{steps: 5_000, faults: [:membership, :drop, :duplicate, :crash, :partition]}

      seeds = TestUtils.failing_seeds(1_201..1_500, opts)

      assert seeds == []
    end

    test "keeps every history linearizable while members come and go" do
      faults = ~w[membership drop crash partition transfer]a
      opts = %{steps: 5_000, workload: :kv, faults: faults}

      results = TestUtils.swept(1_501..1_700, opts)
      found = TestUtils.checked_violations(results)

      assert found == []
    end
  end

  describe "key value bugs" do
    test "catches a dropped put within two hundred seeds" do
      opts = %{steps: 5_000, workload: :kv, bug: :kv_drop_put}

      failing = TestUtils.shortest_failing_run(1..200, opts)

      assert failing != nil
    end

    test "catches a retry that applies twice within five hundred seeds" do
      opts = %{steps: 5_000, workload: :kv, bug: :kv_no_dedupe}

      failing = TestUtils.shortest_failing_run(1..500, opts)

      assert failing != nil
    end
  end

  describe "reads" do
    test "keeps every history linearizable with pauses and transfers" do
      faults = ~w[pause transfer partition crash]a
      opts = %{steps: 5_000, workload: :kv, faults: faults, weights: %{pause: 2, resume: 2}}

      results = TestUtils.swept(1..300, opts)
      found = TestUtils.checked_violations(results)

      assert found == []
    end

    test "catches a read without confirmation within two hundred seeds" do
      weights = %{pause: 1, resume: 1}

      opts = %{
        steps: 5_000,
        workload: :kv,
        bug: :read_without_confirm,
        faults: [:pause],
        weights: weights
      }

      failing = TestUtils.shortest_failing_run(1..200, opts)

      assert failing != nil
    end
  end

  describe "bugs" do
    test "catches a double vote within five hundred seeds" do
      opts = %{steps: 5_000, bug: :double_vote}

      failing = TestUtils.shortest_failing_run(1..500, opts)

      assert failing != nil
    end

    test "catches an ack before durable within five hundred seeds" do
      opts = %{steps: 5_000, bug: :ack_before_durable}

      failing = TestUtils.shortest_failing_run(1..500, opts)

      assert failing != nil
    end

    test "catches an old term commit within five hundred seeds" do
      opts = %{steps: 5_000, bug: :commit_old_term}

      failing = TestUtils.shortest_failing_run(1..500, opts)

      assert failing != nil
    end
  end
end
