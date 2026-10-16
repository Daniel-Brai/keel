defmodule KeelSim.Regression.DisruptiveRejoinTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # A member cut off from the others times out again and again. It comes back and
  # a heartbeat of the healthy leader reaches it. The leader must keep its role.
  @script [
    {:elect, "m1", ~w[m2 m3]},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:isolate, "m3"},
    {:tick, "m3", 100},
    :heal,
    {:tick, "m3", 20},
    {:exchange, "m3", "m1"},
    {:exchange, "m3", "m2"},
    {:tick, "m1", 2},
    {:exchange, "m1", "m3"}
  ]

  @opts %{size: 3, pre_vote: true}

  describe "a member that returns after a partition" do
    test "leaves the leader in its role" do
      cluster = Scenario.play(@script, @opts)

      leader = Scenario.node(cluster, "m1")

      assert {leader.role, leader.term} == {:leader, 1}
    end

    test "keeps its own term" do
      cluster = Scenario.play(@script, @opts)

      returned = Scenario.node(cluster, "m3")

      assert returned.term == 1
    end

    test "follows the leader again" do
      cluster = Scenario.play(@script, @opts)

      returned = Scenario.node(cluster, "m3")

      assert returned.leader_id == "m1"
    end

    test "leaves the invariants clean and the cluster live" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert {result.violations, result.liveness} == {[], []}
    end

    test "gets its pre-votes refused" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.coverage.pre_vote_refused >= 1
    end
  end

  describe "without pre-vote" do
    test "deposes the healthy leader" do
      opts = Map.put(@opts, :bug, :no_pre_vote)
      cluster = Scenario.play(@script, opts)

      deposed = Scenario.node(cluster, "m1")

      assert deposed.role == :follower
    end
  end
end
