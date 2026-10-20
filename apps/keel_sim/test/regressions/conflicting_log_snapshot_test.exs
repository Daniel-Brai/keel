defmodule KeelSim.Regression.ConflictingLogSnapshotTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # The first leader keeps three entries that only it holds. A later leader commits other
  # entries in the same places and snapshots them. The first member returns with its old
  # log and has to take the snapshot in place of it. Replication then goes on from the
  # snapshot with no second snapshot.
  @script [
    {:elect, "m1", ["m2"]},
    {:propose, "m1", "x"},
    {:propose, "m1", "y"},
    {:propose, "m1", "z"},
    {:sync, "m1"},
    {:drop_all, "m1", "m2"},
    {:drop_all, "m1", "m3"},
    {:crash, "m1"},
    {:elect, "m2", ["m3"]},
    {:propose, "m2", "p"},
    {:propose, "m2", "q"},
    {:sync, "m2"},
    {:exchange, "m2", "m3"},
    {:exchange, "m2", "m3"},
    {:exchange, "m2", "m3"},
    {:snapshot, "m2"},
    {:drop_all, "m2", "m1"},
    {:restart, "m1"},
    {:tick, "m2", 2},
    {:exchange, "m2", "m1"},
    {:exchange, "m2", "m1"},
    {:exchange, "m2", "m1"},
    {:exchange, "m2", "m1"},
    {:exchange, "m2", "m1"},
    {:exchange, "m2", "m1"},
    {:snapshot, "m1"},
    {:sync, "m1"},
    {:deliver_all, "m1", "m2"}
  ]

  @opts %{
    size: 3,
    snapshot_threshold_bytes: 1,
    snapshot_trailing_entries: 0,
    snapshot_chunk_bytes: 64,
    snapshot_max_inflight: 2
  }

  describe "snapshot over a conflicting log" do
    test "replaces the log of the follower" do
      cluster = Scenario.play(@script, @opts)

      follower = Scenario.node(cluster, "m1")

      assert {follower.log.first_index, follower.log.last_index} == {4, 3}
    end

    test "raises the commit index of the follower to the snapshot" do
      cluster = Scenario.play(@script, @opts)

      follower = Scenario.node(cluster, "m1")

      assert {follower.commit, follower.applied} == {3, 3}
    end

    test "leaves the invariants clean after the heal" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "leaves the cluster live after the heal" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.liveness == []
    end

    test "installs one snapshot and never starts another send" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert {result.coverage.snapshot_installed, result.coverage.snapshot_send_restarted} ==
               {1, 0}
    end

    test "counts the dropped log suffix" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.coverage.log_suffix_dropped == 1
    end
  end

  describe "snapshot over a conflicting log with the bug" do
    test "reports a log that stayed beside the snapshot" do
      opts = Map.put(@opts, :bug, :keep_log_after_snapshot)
      cluster = Scenario.play(@script, opts)

      result = Scenario.finish(cluster)

      assert {:snapshot_log_mismatch, "m1", 3} in result.violations
    end
  end
end
