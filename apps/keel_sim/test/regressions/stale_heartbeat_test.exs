defmodule KeelSim.Regression.StaleHeartbeatTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # The first leader takes a proposal. The next leader of a higher term then reaches it
  # with a late heartbeat. The deposed leader must drop all leader state.
  @script [
    {:elect, "m1", ["m3"]},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:elect, "m2", ["m3"]},
    {:propose, "m1", "x"},
    {:deliver, "m1", "m3"},
    {:deliver, "m2", "m1"},
    {:deliver_all, "m3", "m1"}
  ]

  describe "late higher term heartbeat" do
    test "leaves the invariants clean" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "leaves the cluster live" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.liveness == []
    end

    test "reached the proposal that lost its leader" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.coverage.leadership_lost_reply >= 1
    end

    test "turns the old leader into a follower" do
      cluster = Scenario.play(@script, %{size: 3})

      deposed = Scenario.node(cluster, "m1")

      assert {deposed.role, deposed.leader, deposed.leader_id} == {:follower, nil, "m2"}
    end

    test "raises the old leader to the new term" do
      cluster = Scenario.play(@script, %{size: 3})

      deposed = Scenario.node(cluster, "m1")
      leader = Scenario.node(cluster, "m2")

      assert deposed.term == leader.term
    end
  end
end
