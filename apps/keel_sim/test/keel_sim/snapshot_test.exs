defmodule KeelSim.SnapshotTest do
  use ExUnit.Case, async: true

  alias KeelSim.TestUtils

  @every_entry %{steps: 3_000, snapshot_threshold_bytes: 1, snapshot_trailing_entries: 0}

  describe "run/1" do
    test "keeps every check when members snapshot after every entry" do
      seeds = TestUtils.violating_seeds(1..30, @every_entry)

      assert seeds == []
    end

    test "keeps every check in five member clusters" do
      opts = Map.put(@every_entry, :size, 5)

      seeds = TestUtils.violating_seeds(301..320, opts)

      assert seeds == []
    end

    test "keeps every check on the chain workload" do
      opts = Map.put(@every_entry, :workload, :chain)

      seeds = TestUtils.violating_seeds(1..20, opts)

      assert seeds == []
    end

    test "leaves every cluster live after the heal" do
      seeds = TestUtils.liveness_failures(1..30, @every_entry)

      assert seeds == []
    end

    test "installs snapshots on members that fell behind" do
      coverage = TestUtils.coverage_after(1..30, @every_entry)

      assert coverage.snapshot_installed > 0
    end

    test "finds a log kept beside a snapshot" do
      opts = Map.put(@every_entry, :bug, :keep_log_after_snapshot)

      seeds = TestUtils.violating_seeds(45..90, opts)

      assert seeds != []
    end

    test "takes snapshots" do
      coverage = TestUtils.coverage_after(1..10, @every_entry)

      assert coverage.snapshot_taken > 0
    end
  end
end
