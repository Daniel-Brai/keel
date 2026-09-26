defmodule Keel.Log.RecoveryTest do
  use ExUnit.Case, async: true

  alias Keel.Log.Recovery
  alias Keel.Log.Recovery.Result
  alias Keel.TestUtils

  describe "recover/2" do
    test "fails for a gap in segment numbers" do
      identity = TestUtils.log_identity()
      first = TestUtils.segment_bytes(1, 1, [])
      third = TestUtils.segment_bytes(3, 1, [])

      recovered = Recovery.recover([{1, first}, {3, third}], identity)

      assert {:error, {:log_corrupt, 3, 0, :segment_gap}} = recovered
    end

    test "fails for a segment of another member" do
      identity = TestUtils.log_identity()
      segment = TestUtils.segment_bytes(1, 1, [])
      other = Map.put(identity, :member_tag, <<1::64>>)

      recovered = Recovery.recover([{1, segment}], other)

      assert {:error, {:log_corrupt, 1, 0, :wrong_identity}} = recovered
    end

    test "fails for a bad record that a later mark covers" do
      identity = TestUtils.log_identity()

      bodies = [
        {:entries, TestUtils.entries([{1, 1}])},
        {:entries, TestUtils.entries([{2, 1}])},
        {:sync_mark, 1, 64, 2}
      ]

      segment = TestUtils.segment_bytes(1, 1, bodies, corrupt: 1)
      offset = TestUtils.record_offset(1, 1, bodies, 1)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:error, {:log_corrupt, 1, ^offset, :bad_crc}} = recovered
    end

    test "fails for a bad record in an older segment" do
      identity = TestUtils.log_identity()
      bodies = [{:entries, TestUtils.entries([{1, 1}])}, {:entries, TestUtils.entries([{2, 1}])}]
      older = TestUtils.segment_bytes(1, 1, bodies, corrupt: 0)
      newer = TestUtils.segment_bytes(2, 3, [])

      recovered = Recovery.recover([{1, older}, {2, newer}], identity)

      assert {:error, {:log_corrupt, 1, 64, :bad_crc}} = recovered
    end

    test "fails for entries that skip an index" do
      identity = TestUtils.log_identity()
      bodies = [{:entries, TestUtils.entries([{1, 1}])}, {:entries, TestUtils.entries([{3, 1}])}]
      segment = TestUtils.segment_bytes(1, 1, bodies)
      offset = TestUtils.record_offset(1, 1, bodies, 1)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:error, {:log_corrupt, 1, ^offset, :index_gap}} = recovered
    end

    test "fails for a segment that starts past the last record" do
      identity = TestUtils.log_identity()
      bodies = [{:entries, TestUtils.entries([{1, 1}])}]
      older = TestUtils.segment_bytes(1, 1, bodies)
      newer = TestUtils.segment_bytes(2, 9, [])
      offset = TestUtils.record_offset(1, 1, bodies, 1)

      recovered = Recovery.recover([{1, older}, {2, newer}], identity)

      assert {:error, {:log_corrupt, 1, ^offset, :end}} = recovered
    end

    test "fails for a bad header beside a readable record" do
      identity = TestUtils.log_identity()
      entries = TestUtils.entries([{1, 1}])
      good = TestUtils.segment_bytes(1, 1, [{:entries, entries}])
      <<_magic::binary-size(8), rest::binary>> = good
      broken = <<"XEELLOG", 0, rest::binary>>

      recovered = Recovery.recover([{1, broken}], identity)

      assert {:error, {:log_corrupt, 1, 0, :bad_magic}} = recovered
    end

    test "recovers nothing from no segments" do
      identity = TestUtils.log_identity()
      recovered = Recovery.recover([], identity)

      assert {:ok, %Result{entries: [], next_seq: 1, last_segment: nil}} = recovered
    end

    test "applies truncates in order" do
      identity = TestUtils.log_identity()

      bodies = [
        {:entries, TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])},
        {:truncate, 2},
        {:entries, TestUtils.entries([{2, 2}])}
      ]

      segment = TestUtils.segment_bytes(1, 1, bodies)
      {:ok, result} = Recovery.recover([{1, segment}], identity)
      terms = TestUtils.pluck(result.entries, :term)

      assert terms == [1, 2]
    end

    test "keeps the last hard state" do
      identity = TestUtils.log_identity()

      bodies = [
        {:hard_state, TestUtils.hard_state(1, "m1", 0)},
        {:hard_state, TestUtils.hard_state(2, nil, 0)}
      ]

      segment = TestUtils.segment_bytes(1, 1, bodies)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:ok, %Result{hard_state: %{term: 2, voted_for: nil}}} = recovered
    end

    test "cuts the commit hint to the last index" do
      identity = TestUtils.log_identity()

      bodies = [
        {:entries, TestUtils.entries([{1, 1}])},
        {:hard_state, TestUtils.hard_state(1, nil, 5)}
      ]

      segment = TestUtils.segment_bytes(1, 1, bodies)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:ok, %Result{hard_state: %{commit: 1}}} = recovered
    end

    test "treats a torn record as the tail" do
      identity = TestUtils.log_identity()
      bodies = [{:entries, TestUtils.entries([{1, 1}])}, {:entries, TestUtils.entries([{2, 1}])}]
      segment = TestUtils.segment_bytes(1, 1, bodies)
      torn = binary_part(segment, 0, byte_size(segment) - 5)

      {:ok, result} = Recovery.recover([{1, torn}], identity)
      indexes = TestUtils.pluck(result.entries, :index)

      assert {indexes, result.torn_tail?, result.next_seq} == {[1], true, 2}
    end

    test "treats a bad record that no mark covers as the tail" do
      identity = TestUtils.log_identity()

      bodies = [
        {:entries, TestUtils.entries([{1, 1}])},
        {:sync_mark, 1, 64, 0},
        {:entries, TestUtils.entries([{2, 1}])}
      ]

      segment = TestUtils.segment_bytes(1, 1, bodies, corrupt: 2)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:ok, %Result{torn_tail?: true, next_seq: 3}} = recovered
    end

    test "treats zero fill as a clean end" do
      identity = TestUtils.log_identity()
      entries = TestUtils.entries([{1, 1}])
      segment = TestUtils.segment_bytes(1, 1, [{:entries, entries}], pad: 4_096)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:ok, %Result{torn_tail?: false, entries: [_entry]}} = recovered
    end

    test "returns the records after the last mark as the tail" do
      identity = TestUtils.log_identity()

      bodies = [
        {:entries, TestUtils.entries([{1, 1}])},
        {:sync_mark, 1, 64, 1},
        {:entries, TestUtils.entries([{2, 1}])}
      ]

      segment = TestUtils.segment_bytes(1, 1, bodies)
      {:ok, result} = Recovery.recover([{1, segment}], identity)
      seqs = TestUtils.pluck(result.tail, 0)

      assert seqs == [2, 3]
    end

    test "prefers the newer copy of a record" do
      identity = TestUtils.log_identity()

      old_bodies = [
        {:entries, TestUtils.entries([{1, 1}, {2, 1}])},
        {:entries, TestUtils.entries([{3, 1}])}
      ]

      new_bodies = [{:entries, TestUtils.entries([{3, 7}])}]
      older = TestUtils.segment_bytes(1, 1, old_bodies)
      newer = TestUtils.segment_bytes(2, 2, new_bodies)

      {:ok, result} = Recovery.recover([{1, older}, {2, newer}], identity)
      terms = TestUtils.pluck(result.entries, :term)

      assert terms == [1, 1, 7]
    end

    test "drops a newest segment with an empty header" do
      identity = TestUtils.log_identity()
      good = TestUtils.segment_bytes(1, 1, [])
      blank = <<0::size(512)>>

      recovered = Recovery.recover([{1, good}, {2, blank}], identity)

      assert {:ok, %Result{dropped: [2], last_segment: 1}} = recovered
    end
  end

  describe "recover/3 with compaction" do
    test "fails for entries dropped with no snapshot" do
      identity = TestUtils.log_identity()
      bodies = [{:entries, TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])}, {:compacted, 3}]
      segment = TestUtils.segment_bytes(1, 1, bodies)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:error, {:log_corrupt, 1, 0, :compacted_without_snapshot}} = recovered
    end

    test "fails when the compaction passes the snapshot" do
      identity = TestUtils.log_identity()
      meta = TestUtils.snapshot_meta(2, 1)
      bodies = [{:entries, TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])}, {:compacted, 5}]
      segment = TestUtils.segment_bytes(1, 1, bodies)

      recovered = Recovery.recover([{1, segment}], identity, meta)

      assert {:error, {:log_corrupt, 1, 0, {:compacted_past_snapshot, ^meta}}} = recovered
    end

    test "fails for entries that start past a prefix nothing explains" do
      identity = TestUtils.log_identity()
      bodies = [{:entries, TestUtils.entries([{5, 1}, {6, 1}])}]
      segment = TestUtils.segment_bytes(1, 1, bodies)

      recovered = Recovery.recover([{1, segment}], identity)

      assert {:error, {:log_corrupt, 1, 0, :missing_prefix}} = recovered
    end

    test "accepts entries that start after a deleted prefix" do
      identity = TestUtils.log_identity()
      meta = TestUtils.snapshot_meta(4, 1)
      bodies = [{:entries, TestUtils.entries([{5, 1}, {6, 1}])}, {:compacted, 5}]
      segment = TestUtils.segment_bytes(3, 9, bodies)

      {:ok, result} = Recovery.recover([{3, segment}], identity, meta)

      assert {result.first_index, TestUtils.pluck(result.entries, :index)} == {5, [5, 6]}
    end

    test "drops a log that conflicts with a newer snapshot" do
      identity = TestUtils.log_identity()
      meta = TestUtils.snapshot_meta(10, 3)
      bodies = [{:entries, TestUtils.switching_entries(14, 10, 1, 2)}]
      segment = TestUtils.segment_bytes(1, 1, bodies)

      {:ok, result} = Recovery.recover([{1, segment}], identity, meta)

      assert {result.first_index, result.entries} == {11, []}
    end

    test "keeps the entries after a snapshot the log was compacted against" do
      identity = TestUtils.log_identity()
      meta = TestUtils.snapshot_meta(10, 3)
      entries = TestUtils.switching_entries(14, 10, 1, 2)
      bodies = [{:entries, entries}, {:compacted, 11}]
      segment = TestUtils.segment_bytes(1, 1, bodies)

      {:ok, result} = Recovery.recover([{1, segment}], identity, meta)

      assert {result.first_index, TestUtils.pluck(result.entries, :index)} ==
               {11, [11, 12, 13, 14]}
    end

    test "keeps trailing entries when the log holds the snapshot entry" do
      identity = TestUtils.log_identity()
      meta = TestUtils.snapshot_meta(10, 3)
      entries = TestUtils.switching_entries(14, 10, 1, 3)
      bodies = [{:entries, entries}, {:compacted, 4}]
      segment = TestUtils.segment_bytes(1, 1, bodies)

      {:ok, result} = Recovery.recover([{1, segment}], identity, meta)

      assert {result.first_index, length(result.entries)} == {4, 11}
    end

    test "drops trailing entries when the snapshot entry has another term" do
      identity = TestUtils.log_identity()
      meta = TestUtils.snapshot_meta(10, 3)
      entries = TestUtils.switching_entries(14, 10, 1, 2)
      bodies = [{:entries, entries}, {:compacted, 4}]
      segment = TestUtils.segment_bytes(1, 1, bodies)

      {:ok, result} = Recovery.recover([{1, segment}], identity, meta)

      assert {result.first_index, result.entries} == {11, []}
    end

    test "reports the newest entry of each segment" do
      identity = TestUtils.log_identity()
      first = TestUtils.segment_bytes(1, 1, [{:entries, TestUtils.entries([{1, 1}, {2, 1}])}])
      second = TestUtils.segment_bytes(2, 2, [{:entries, TestUtils.entries([{3, 1}])}])

      {:ok, result} = Recovery.recover([{1, first}, {2, second}], identity)

      assert result.segments == [{1, 2}, {2, 3}]
    end
  end
end
