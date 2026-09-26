defmodule Keel.Log.StoreTest do
  use ExUnit.Case, async: true

  alias Keel.Env.Disk.Posix
  alias Keel.Log.Recovery.Result
  alias Keel.Log.Store
  alias Keel.Raft.Entry
  alias Keel.TestUtils

  @moduletag :tmp_dir

  describe "open/3" do
    test "fails for a log of another member", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      TestUtils.open_store!(disk, log, member: "m1")

      opened = Store.open(disk, log, member: "m2")

      assert {:error, %Posix{}, {:log_corrupt, 1, 0, :wrong_identity}} = opened
    end

    test "creates a missing directory", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")

      opened = Store.open(disk, log, [])

      assert {:ok, %Store{}, %Result{entries: [], next_seq: 1}} = opened
    end

    test "recovers what an earlier store wrote", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      hard_state = TestUtils.hard_state(3, "m2", 0)
      entries = TestUtils.entries([{1, 1}, {2, 3}])
      ops = [{:entries, entries}, {:hard_state, hard_state}]

      {store, _result} = TestUtils.open_store!(disk, log)
      TestUtils.write_synced!(store, ops)

      {_reopened, result} = TestUtils.open_store!(disk, log)
      terms = TestUtils.pluck(result.entries, :term)

      assert {terms, result.hard_state} == {[1, 3], hard_state}
    end

    test "copies the unproven tail into a fresh segment", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      entries = TestUtils.entries([{1, 1}])

      {store, _result} = TestUtils.open_store!(disk, log)
      TestUtils.write_synced!(store, [{:entries, entries}])
      TestUtils.open_store!(disk, log)
      files = TestUtils.segment_files(log)

      assert files == ~w[00000001.seg 00000002.seg]
    end

    test "recovers the same state twice", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      entries = TestUtils.entries([{1, 1}, {2, 1}])

      {store, _result} = TestUtils.open_store!(disk, log)
      TestUtils.write_synced!(store, [{:entries, entries}])
      {_first, older} = TestUtils.open_store!(disk, log)

      {_second, newer} = TestUtils.open_store!(disk, log)

      assert newer.entries == older.entries
    end
  end

  describe "exists?/2" do
    test "returns false for a missing directory", %{tmp_dir: dir} do
      log = Path.join(dir, "log")

      found = Store.exists?(Posix.new(), log)

      assert {:ok, %Posix{}, false} = found
    end

    test "returns true once a segment was created", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      TestUtils.open_store!(disk, log)

      found = Store.exists?(disk, log)

      assert {:ok, %Posix{}, true} = found
    end
  end

  describe "bootstrap/3" do
    test "keeps the first entry after a reopen", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      {:ok, store, result} = Store.open(disk, log, [])
      entry = TestUtils.config_entry(1, 0, TestUtils.random_config())
      {:ok, _store, bootstrapped} = Store.bootstrap(store, result, entry)

      {:ok, _store, recovered} = Store.open(disk, log, [])

      assert {bootstrapped.entries, recovered.entries} == {[entry], [entry]}
    end
  end

  describe "write/2" do
    test "fails for a truncation of committed entries", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")

      committed = [
        {:entries, TestUtils.entries([{1, 1}, {2, 1}])},
        {:hard_state, TestUtils.hard_state(1, nil, 2)}
      ]

      {store, _result} = TestUtils.open_store!(disk, log)
      written = TestUtils.write_synced!(store, committed)

      failed = Store.write(written, [{:truncate_from, 2}])

      assert {:error, %Store{}, :truncate_committed} = failed
    end

    test "fails after a failure poisoned the store", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      hard_state = TestUtils.hard_state(2, nil, 1)

      committed = [
        {:entries, TestUtils.entries([{1, 1}])},
        {:hard_state, TestUtils.hard_state(1, nil, 1)}
      ]

      {store, _result} = TestUtils.open_store!(disk, log)
      written = TestUtils.write_synced!(store, committed)
      {:error, poisoned, _reason} = Store.write(written, [{:truncate_from, 1}])

      failed = Store.write(poisoned, [{:hard_state, hard_state}])

      assert {:error, %Store{}, {:poisoned, :truncate_committed}} = failed
    end

    test "fails for an entry over the record limit", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      big = %Entry{index: 1, term: 1, kind: :command, data: :binary.copy("x", 100), time_ms: 0}

      {store, _result} = TestUtils.open_store!(disk, log, max_record_bytes: 64)

      failed = Store.write(store, [{:entries, [big]}])

      assert {:error, %Store{}, :entry_too_large} = failed
    end

    test "rotates into a new segment", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")

      batches =
        Enum.map(0..9, fn batch ->
          entries = TestUtils.many_entries(batch * 3 + 1, 3, 1)

          [{:entries, entries}]
        end)

      {store, _result} = TestUtils.open_store!(disk, log, segment_bytes: 512)
      TestUtils.write_batches!(store, batches)

      {_reopened, result} = TestUtils.open_store!(disk, log, segment_bytes: 512)
      indexes = TestUtils.pluck(result.entries, :index)

      expected = Enum.to_list(1..30)

      assert indexes == expected
    end

    test "gives a batch larger than a segment its own segment", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      entries = TestUtils.many_entries(1, 20, 1)

      {store, _result} = TestUtils.open_store!(disk, log, segment_bytes: 256)
      TestUtils.write_synced!(store, [{:entries, entries}])

      {_reopened, result} = TestUtils.open_store!(disk, log, segment_bytes: 256)
      count = length(result.entries)

      assert count == 20
    end

    test "writes a truncate before the entries that replace the tail", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      old = TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])
      new = TestUtils.entries([{2, 2}])

      {store, _result} = TestUtils.open_store!(disk, log)
      first = TestUtils.write_synced!(store, [{:entries, old}])
      TestUtils.write_synced!(first, [{:truncate_from, 2}, {:entries, new}])

      {_reopened, result} = TestUtils.open_store!(disk, log)
      terms = TestUtils.pluck(result.entries, :term)

      assert terms == [1, 2]
    end
  end

  describe "sync/1" do
    test "marks the store clean", %{tmp_dir: dir} do
      disk = Posix.new()
      log = Path.join(dir, "log")
      entries = TestUtils.entries([{1, 1}])

      {store, _result} = TestUtils.open_store!(disk, log)
      {:ok, written} = Store.write(store, [{:entries, entries}])
      {:ok, synced} = Store.sync(written)

      before = Store.dirty?(written)
      after_sync = Store.dirty?(synced)

      assert {before, after_sync} == {true, false}
    end
  end

  describe "take_compacted/1" do
    test "returns nothing before the compaction is synced", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      filled = TestUtils.filled_store!(Posix.new(), log)
      {:ok, written} = Store.write(filled, [{:compact_log, 25}])

      {paths, _taken} = Store.take_compacted(written)

      assert paths == []
    end

    test "leaves the compacted segments on disk after a sync", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      filled = TestUtils.filled_store!(Posix.new(), log)
      before = TestUtils.segment_files(log)

      TestUtils.write_synced!(filled, [{:compact_log, 25}])
      files = TestUtils.segment_files(log)

      assert files == before
    end

    test "returns the compacted segments oldest first", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      filled = TestUtils.filled_store!(Posix.new(), log)
      compacted = TestUtils.write_synced!(filled, [{:compact_log, 25}])

      {paths, _taken} = Store.take_compacted(compacted)
      names = Enum.map(paths, &Path.basename/1)

      assert [_first | _rest] = names
      assert names == Enum.sort(names)
    end
  end

  describe "discard/3" do
    test "moves the taken segments into the trash", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      filled = TestUtils.filled_store!(Posix.new(), log)
      compacted = TestUtils.write_synced!(filled, [{:compact_log, 25}])
      {paths, _taken} = Store.take_compacted(compacted)

      {:ok, _disk, trashed} = Store.discard(Posix.new(), log, paths)
      left = Enum.filter(paths, &File.exists?/1)
      moved = Enum.filter(trashed, &File.exists?/1)

      assert {left, moved} == {[], trashed}
    end

    test "leaves trash that recovery never reads", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      TestUtils.filled_store!(Posix.new(), log)
      trash = Path.join(log, "trash")
      junk = Path.join(trash, "00000001.seg")
      File.mkdir_p!(trash)
      File.write!(junk, "not a segment")

      reopened = Store.open(Posix.new(), log, segment_bytes: 512)
      {:ok, _disk, listed} = Store.trash(Posix.new(), log)

      assert {:ok, %Store{}, _result} = reopened
      assert listed == [junk]
    end
  end
end
