defmodule Keel.Raft.LogTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Keel.Raft.Log
  alias Keel.TestUtils

  describe "term_at/2" do
    test "returns nil below the entry before the first one" do
      log = 6 |> TestUtils.terms_to(1, 1, 1) |> TestUtils.entries() |> Log.new()
      compacted = Log.compact(log, TestUtils.snapshot_meta(6, 1), 5)

      term = Log.term_at(compacted, 3)

      assert term == nil
    end

    test "answers for the entry just below the first one" do
      log = 6 |> TestUtils.terms_to(4, 1, 2) |> TestUtils.entries() |> Log.new()
      compacted = Log.compact(log, TestUtils.snapshot_meta(6, 2), 5)

      term = Log.term_at(compacted, 4)

      assert term == 2
    end
  end

  describe "compact/3" do
    test "keeps the entries from the keep point" do
      log = 6 |> TestUtils.terms_to(1, 1, 1) |> TestUtils.entries() |> Log.new()

      compacted = Log.compact(log, TestUtils.snapshot_meta(6, 1), 5)

      assert {compacted.first_index, TestUtils.kept_indexes(compacted)} == {5, [5, 6]}
    end

    test "never moves the first index back" do
      log = 6 |> TestUtils.terms_to(1, 1, 1) |> TestUtils.entries() |> Log.new()
      once = Log.compact(log, TestUtils.snapshot_meta(6, 1), 5)

      twice = Log.compact(once, TestUtils.snapshot_meta(6, 1), 2)

      assert twice.first_index == 5
    end
  end

  describe "reset/2" do
    test "leaves an empty log that ends at the snapshot" do
      log = [{1, 1}, {2, 1}] |> TestUtils.entries() |> Log.new()

      reset = Log.reset(log, TestUtils.snapshot_meta(10, 3))
      held = TestUtils.kept_indexes(reset)

      assert {reset.first_index, reset.last_index, reset.last_term, held} == {11, 10, 3, []}
    end
  end

  describe "restore/3" do
    test "drops the first entry when the entry before it is unknown" do
      entries = TestUtils.run_entries(5, 9, 1)
      meta = TestUtils.snapshot_meta(8, 1)

      log = Log.restore(entries, 5, meta)

      assert {log.first_index, log.last_index} == {6, 9}
    end

    test "starts right after a snapshot that ends the log" do
      meta = TestUtils.snapshot_meta(8, 4)

      log = Log.restore([], 9, meta)

      assert {log.first_index, log.last_index, log.last_term, log.snapshot_index} == {9, 8, 4, 8}
    end

    test "keeps a whole log that starts at one" do
      entries = TestUtils.run_entries(1, 9, 1)
      meta = TestUtils.snapshot_meta(8, 1)

      log = Log.restore(entries, 1, meta)

      assert {log.first_index, log.last_index, log.snapshot_index} == {1, 9, 8}
    end
  end

  describe "truncate_from/2" do
    test "falls back to the time of the entry below the first one" do
      log = 6 |> TestUtils.terms_to(1, 1, 1) |> TestUtils.entries() |> Log.new()
      compacted = Log.compact(log, TestUtils.snapshot_meta(6, 1), 5)

      truncated = Log.truncate_from(compacted, 5)

      assert {truncated.last_index, truncated.last_time_ms} == {4, 40}
    end
  end

  describe "packing" do
    property "answers the same as a log that never packs" do
      check all(ops <- TestUtils.random_log_ops()) do
        packed = TestUtils.play_log_ops(ops, 4)
        plain = TestUtils.play_log_ops(ops, 1_000_000)

        assert TestUtils.log_view(packed) == TestUtils.log_view(plain)
      end
    end

    test "keeps only the newest entries as structs" do
      entries = TestUtils.run_entries(1, 3_000, 1)

      log = Log.new(entries)

      assert {map_size(log.recent), map_size(log.chunks)} == {1_976, 1}
    end
  end
end
