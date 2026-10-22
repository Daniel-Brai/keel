defmodule KeelSim.Regression.DeposedLeaderReadTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # Both clients register with the first leader. A partition cuts that leader off.
  # The next leader commits a write. A read that reaches the old leader must not be answered.
  @script [
    {:elect, "m1", ~w[m2 m3]},
    {:client, 1, :get, {:shared, 0}, "m1"},
    {:client, 2, :get, {:shared, 0}, "m1"},
    {:sync, "m1"},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:isolate, "m1"},
    {:elect, "m2", ["m3"]},
    {:client, 2, :put, {:own, 0}, "m2"},
    {:sync, "m2"},
    {:exchange, "m2", "m3"},
    {:client, 1, :get, {:own, 0}, "m1"}
  ]

  @opts %{size: 3, workload: :kv, clients: 2}

  describe "a leader cut off by a partition" do
    test "does not confirm the read" do
      cluster = Scenario.play(@script, @opts)

      deposed = Scenario.node(cluster, "m1")
      client = cluster.clients[1]

      assert {deposed.role, client.pending.type} == {:leader, :op}
    end

    test "leaves the invariants clean after the heal" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "answers the read once the cluster is whole" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.liveness == []
    end
  end

  describe "with a leader that skips the confirmation" do
    test "serves the read from a stale state" do
      opts = Map.put(@opts, :bug, :read_without_confirm)
      cluster = Scenario.play(@script, opts)

      result = Scenario.finish(cluster)

      assert {:stale_leader_read, "m1", 1} in result.violations
    end
  end
end
