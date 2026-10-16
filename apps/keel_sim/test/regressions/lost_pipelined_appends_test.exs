defmodule KeelSim.Regression.LostPipelinedAppendsTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # The leader sends appends to a follower without waiting for acks. The network
  # loses them. The next append is refused and the leader must send them again.
  @pipelining [
    {:elect, "m1", ["m2"]},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:propose, "m1", "a"},
    {:sync, "m1"},
    {:exchange, "m1", "m2"}
  ]

  @lost [
    {:propose, "m1", "b"},
    {:propose, "m1", "c"},
    {:drop_all, "m1", "m2"},
    {:propose, "m1", "d"},
    {:sync, "m1"},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"}
  ]

  describe "pipelined appends that the network loses" do
    test "keep the invariants clean" do
      cluster = Scenario.play(@pipelining ++ @lost, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "are sent again until the follower holds them" do
      cluster = Scenario.play(@pipelining ++ @lost, %{size: 3})

      leader = Scenario.node(cluster, "m1")
      follower = Scenario.node(cluster, "m2")

      assert follower.log.last_index == leader.log.last_index
    end

    test "converge after the run" do
      cluster = Scenario.play(@pipelining ++ @lost, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.liveness == []
    end

    test "start once the follower has acked" do
      cluster = Scenario.play(@pipelining, %{size: 3})

      leader = Scenario.node(cluster, "m1")

      assert leader.leader.progress["m2"].pipelining?
    end
  end
end
