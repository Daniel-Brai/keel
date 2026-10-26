defmodule KeelSim.MembershipTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario
  alias KeelSim.TestUtils

  @faults ~w[membership drop duplicate crash partition]a

  @paths ~w[
    learner_added
    learner_promoted
    voter_removed
    self_removed_leader_stepped_down
    change_in_progress_refused
    change_before_term_commit_refused
  ]a

  describe "run/1" do
    test "keeps every check while members come and go" do
      opts = %{steps: 3_000, faults: @faults}

      seeds = TestUtils.failing_seeds(1..40, opts)

      assert seeds == []
    end

    test "keeps every check on the chain workload while members come and go" do
      opts = %{steps: 3_000, faults: @faults, workload: :chain}

      seeds = TestUtils.failing_seeds(41..70, opts)

      assert seeds == []
    end

    test "keeps every check with a pre-vote that stays off" do
      opts = %{steps: 3_000, faults: @faults, pre_vote: false}

      seeds = TestUtils.failing_seeds(71..100, opts)

      assert seeds == []
    end

    test "reaches every membership path" do
      coverage = TestUtils.coverage_after(1..90, %{steps: 3_000, faults: @faults})

      unreached = TestUtils.unreached(coverage, @paths)

      assert unreached == []
    end

    test "grows the cluster beyond its first members" do
      opts = %{steps: 3_000, size: 3, faults: @faults}

      seeds = TestUtils.grown_seeds(1..20, opts)

      assert seeds != []
    end

    test "never changes membership without the fault" do
      opts = %{steps: 2_000, faults: [:drop, :crash]}

      seeds = TestUtils.grown_seeds(1..10, opts)

      assert seeds == []
    end
  end

  describe "a scripted change" do
    test "adds a learner that then holds the log" do
      script = [
        {:join, "m4"},
        {:elect, "m1", ["m2"]},
        {:sync, "m1"},
        {:exchange, "m1", "m2"},
        {:tick, "m1", 2},
        {:exchange, "m1", "m2"},
        {:change, "m1", {:add_learner, "m4"}},
        {:sync, "m1"},
        {:exchange, "m1", "m2"},
        {:exchange, "m1", "m4"},
        {:tick, "m1", 2},
        {:exchange, "m1", "m4"}
      ]

      cluster = Scenario.play(script, %{size: 3})
      learner = Scenario.node(cluster, "m4")

      assert learner.log.last_index == 2
    end
  end
end
