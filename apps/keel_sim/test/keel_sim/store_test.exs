defmodule KeelSim.StoreTest do
  use ExUnit.Case, async: true

  alias Keel.Log.Store
  alias KeelSim.Disk
  alias KeelSim.TestUtils

  describe "sync/1" do
    test "poisons the store when the sync fails" do
      store = TestUtils.unsynced_store!()
      faulty = TestUtils.fail_next!(store, :fail_next_sync)

      synced = Store.sync(faulty)

      assert {:error, %Store{}, {:sync_failed, :eio}} = synced
    end

    test "refuses a second sync after a failure" do
      store = TestUtils.unsynced_store!()
      faulty = TestUtils.fail_next!(store, :fail_next_sync)
      {:error, poisoned, _reason} = Store.sync(faulty)

      synced = Store.sync(poisoned)

      assert {:error, %Store{}, {:poisoned, {:sync_failed, :eio}}} = synced
    end

    test "keeps the failed sync from being reported durable" do
      store = TestUtils.unsynced_store!()
      faulty = TestUtils.fail_next!(store, :fail_next_sync)
      {:error, poisoned, _reason} = Store.sync(faulty)

      result = TestUtils.crash_and_reopen!(poisoned)

      assert result.entries == []
    end
  end

  describe "write/2" do
    test "poisons the store when the write fails" do
      {store, _result} = TestUtils.open_sim_store!(Disk.new())
      faulty = TestUtils.fail_next!(store, :fail_next_write)
      entries = TestUtils.entries([{1, 1}])
      ops = [{:entries, entries}]

      written = Store.write(faulty, ops)

      assert {:error, %Store{}, {:write_failed, :eio}} = written
    end

    test "refuses a write after the store is poisoned" do
      {store, _result} = TestUtils.open_sim_store!(Disk.new())
      faulty = TestUtils.fail_next!(store, :fail_next_write)
      entries = TestUtils.entries([{1, 1}])
      ops = [{:entries, entries}]
      {:error, poisoned, _reason} = Store.write(faulty, ops)

      written = Store.write(poisoned, ops)

      assert {:error, %Store{}, {:poisoned, {:write_failed, :eio}}} = written
    end
  end

  describe "open/3" do
    test "recovers synced entries after a strict crash" do
      disk = Disk.new(profile: :strict)
      {store, _result} = TestUtils.open_sim_store!(disk)
      entries = TestUtils.entries([{1, 1}, {2, 1}])
      ops = [{:entries, entries}]
      synced = TestUtils.write_synced!(store, ops)

      result = TestUtils.crash_and_reopen!(synced)

      indexes = TestUtils.indexes(result)

      assert indexes == [1, 2]
    end

    test "loses unsynced entries after a strict crash" do
      disk = Disk.new(profile: :strict)
      {store, _result} = TestUtils.open_sim_store!(disk)
      first_entries = TestUtils.entries([{1, 1}])
      second_entries = TestUtils.entries([{2, 1}])
      first = [{:entries, first_entries}]
      second = [{:entries, second_entries}]
      synced = TestUtils.write_synced!(store, first)
      written = TestUtils.write!(synced, second)

      result = TestUtils.crash_and_reopen!(written)

      indexes = TestUtils.indexes(result)

      assert indexes == [1]
    end

    test "keeps a synced vote after any crash" do
      disk = Disk.new(profile: :torn, seed: {4, 5, 6})
      {store, _result} = TestUtils.open_sim_store!(disk)
      first_state = TestUtils.hard_state(3, "m2")
      second_state = TestUtils.hard_state(4, "m3")
      first = [{:hard_state, first_state}]
      second = [{:hard_state, second_state}]
      synced = TestUtils.write_synced!(store, first)
      written = TestUtils.write!(synced, second)

      result = TestUtils.crash_and_reopen!(written)

      assert result.hard_state.term >= 3
    end

    test "recovers a state the member had on a strict disk" do
      seeds = TestUtils.crash_violations(:strict, 1..40)

      assert seeds == []
    end

    test "recovers a state the member had on a prefix disk" do
      seeds = TestUtils.crash_violations(:prefix, 1..40)

      assert seeds == []
    end

    test "recovers a state the member had on a subset disk" do
      seeds = TestUtils.crash_violations(:subset, 1..40)

      assert seeds == []
    end

    test "recovers a state the member had on a torn disk" do
      seeds = TestUtils.crash_violations(:torn, 1..80)

      assert seeds == []
    end
  end

  describe "compaction" do
    test "keeps the whole log when the crash comes before the sync" do
      entries = TestUtils.many_entries(1, 20, 1)
      store = TestUtils.store_with_entries!(entries)
      {snapshotted, _meta} = TestUtils.snapshot!(store, 15, 1)
      written = TestUtils.compact_unsynced!(snapshotted, 16)

      result = TestUtils.crash_and_reopen!(written)

      assert {result.first_index, length(result.entries)} == {1, 20}
    end

    test "keeps the entries from the keep point after the sync" do
      entries = TestUtils.many_entries(1, 20, 1)
      store = TestUtils.store_with_entries!(entries)
      {snapshotted, _meta} = TestUtils.snapshot!(store, 15, 1)
      synced = TestUtils.compact_synced!(snapshotted, 16)

      result = TestUtils.crash_and_reopen!(synced)

      assert {result.first_index, hd(result.entries).index, length(result.entries)} == {16, 16, 5}
    end

    test "deletes the segments that lie wholly below the keep point" do
      entries = TestUtils.many_entries(1, 20, 1)
      store = TestUtils.store_with_entries!(entries)
      before = TestUtils.segment_count(store)
      {snapshotted, _meta} = TestUtils.snapshot!(store, 15, 1)

      synced = TestUtils.compact_synced!(snapshotted, 16)

      assert TestUtils.segment_count(synced) < before
    end

    test "recovers when a crash leaves only some old segments deleted" do
      entries = TestUtils.many_entries(1, 20, 1)
      store = TestUtils.store_with_entries!(entries)
      {snapshotted, _meta} = TestUtils.snapshot!(store, 15, 1)
      halfway = TestUtils.compact_halfway!(snapshotted, 16)

      result = TestUtils.crash_and_reopen!(halfway)

      assert {result.first_index, hd(result.entries).index} == {16, 16}
    end

    test "fails to start when entries were dropped with no snapshot to cover them" do
      entries = TestUtils.many_entries(1, 20, 1)
      store = TestUtils.store_with_entries!(entries)
      synced = TestUtils.compact_synced!(store, 16)
      disk = synced |> Store.disk() |> Disk.crash()

      opened = Store.open(disk, "/sim/log", segment_bytes: 400)

      assert {:error, _disk, {:log_corrupt, _segment, 0, :compacted_without_snapshot}} = opened
    end
  end

  describe "a snapshot beside an older log" do
    test "drops a log that conflicts with the snapshot after a crash" do
      entries = TestUtils.many_entries(1, 14, 2)
      store = TestUtils.store_with_entries!(entries)
      {snapshotted, meta} = TestUtils.snapshot!(store, 10, 3)

      result = TestUtils.crash_and_reopen!(snapshotted)

      assert {result.first_index, result.entries, result.snapshot} == {11, [], meta}
    end

    test "writes the drop down so the next entries follow it" do
      entries = TestUtils.many_entries(1, 14, 2)
      store = TestUtils.store_with_entries!(entries)
      {snapshotted, _meta} = TestUtils.snapshot!(store, 10, 3)

      {reopened, _result} =
        snapshotted
        |> Store.disk()
        |> Disk.crash()
        |> TestUtils.open_sim_store!(segment_bytes: 400)

      next = TestUtils.many_entries(11, 12, 3)
      ops = [{:entries, next}]
      synced = TestUtils.write_synced!(reopened, ops)

      result = TestUtils.crash_and_reopen!(synced)

      assert TestUtils.indexes(result) == [11, 12]
    end

    test "keeps the log when it holds the last entry of the snapshot" do
      entries = TestUtils.many_entries(1, 14, 3)
      store = TestUtils.store_with_entries!(entries)
      {snapshotted, _meta} = TestUtils.snapshot!(store, 10, 3)

      result = TestUtils.crash_and_reopen!(snapshotted)

      assert {result.first_index, length(result.entries)} == {1, 14}
    end

    test "keeps the entries after a snapshot the log was compacted against" do
      entries = TestUtils.many_entries(1, 14, 2)
      store = TestUtils.store_with_entries!(entries)
      {snapshotted, _meta} = TestUtils.snapshot!(store, 10, 3)
      synced = TestUtils.compact_synced!(snapshotted, 11)

      result = TestUtils.crash_and_reopen!(synced)

      assert TestUtils.indexes(result) == [11, 12, 13, 14]
    end
  end
end
