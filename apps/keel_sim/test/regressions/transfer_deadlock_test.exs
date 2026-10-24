defmodule KeelSim.Regression.TransferDeadlockTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # The leader starts a transfer to a member that is behind. It loses its quorum and
  # steps down. It wins a later election and must still commit.
  @script [
    {:elect, "m1", ["m2"]},
    {:exchange, "m1", "m2"},
    {:transfer, "m1", "m3"},
    {:isolate, "m1"},
    {:tick, "m1", 10},
    :heal,
    {:tick, "m2", 5},
    {:drop_all, "m2", "m1"},
    {:drop_all, "m2", "m3"},
    {:elect, "m1", ["m2"]},
    {:propose, "m1", "x"},
    {:sync, "m1"},
    {:exchange, "m1", "m2"}
  ]

  @opts %{size: 3, pre_vote: true, election_ticks_min: 5, election_ticks_max: 20}

  describe "a leader that stepped down during a transfer" do
    test "steps down when it loses its quorum" do
      script = Enum.take(@script, 5)
      cluster = Scenario.play(script, @opts)

      stepped = Scenario.node(cluster, "m1")

      assert {stepped.role, stepped.leader} == {:follower, nil}
    end

    test "was holding the transfer before" do
      script = Enum.take(@script, 4)
      cluster = Scenario.play(script, @opts)

      leader = Scenario.node(cluster, "m1")

      assert %{target: "m3"} = leader.leader.transfer
    end

    test "leads again with no transfer" do
      cluster = Scenario.play(@script, @opts)

      leader = Scenario.node(cluster, "m1")

      assert {leader.role, leader.leader.transfer} == {:leader, nil}
    end

    test "commits a proposal in the new term" do
      cluster = Scenario.play(@script, @opts)

      leader = Scenario.node(cluster, "m1")

      assert leader.commit == leader.log.last_index
    end

    test "leaves the invariants clean and the cluster live" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert {result.violations, result.liveness} == {[], []}
    end
  end
end
