defmodule KeelSim.Regression.LostUnsyncedEntriesTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # A follower holds entries it never synced and crashes. The entries commit on the
  # other two members. The follower returns without them and must catch up.
  @script [
    {:elect, "m1", ["m2"]},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:propose, "m1", "a"},
    {:propose, "m1", "b"},
    {:sync, "m1"},
    {:deliver_all, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:crash, "m2"},
    {:restart, "m2"}
  ]

  describe "follower that lost unsynced entries" do
    test "keeps the invariants clean" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "converges with the leader" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.liveness == []
    end

    test "returns without the entries it never synced" do
      cluster = Scenario.play(@script, %{size: 3})

      member = Scenario.node(cluster, "m2")

      assert member.log.last_index == 1
    end

    test "reached a crash with pending writes" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.coverage.crash_with_pending_writes >= 1
    end

    test "commits the entries on the other members" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.committed >= 3
    end
  end
end
