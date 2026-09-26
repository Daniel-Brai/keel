defmodule Keel.Log.WriterTest do
  use ExUnit.Case, async: true

  alias Keel.Env.Disk.Stub, as: DiskStub
  alias Keel.Log.Recovery.Result
  alias Keel.Log.Writer
  alias Keel.TestUtils

  @moduletag :tmp_dir
  @moduletag :capture_log

  describe "start_link/1" do
    test "fails for a log of another member", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      disk = DiskStub.new()
      owner = self()
      TestUtils.start_writer!(log, member: "m1")
      Process.flag(:trap_exit, true)

      started = Writer.start_link(disk: disk, dir: log, owner: owner, store: [member: "m2"])

      assert {:error, {:open_failed, {:log_corrupt, 1, 0, :wrong_identity}}} = started
    end
  end

  describe "start_link/1 for a new member" do
    test "writes the first entry when it bootstraps", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      config = TestUtils.random_config()
      writer = TestUtils.start_writer!(log, bootstrap: config)

      %Result{entries: [entry]} = Writer.recovered(writer)

      assert {entry.index, entry.term, entry.kind} == {1, 0, :config}
    end

    test "leaves an existing log as it is when it bootstraps again", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      config = TestUtils.random_config()
      first = TestUtils.start_writer!(log, bootstrap: config)
      before = Writer.recovered(first)
      GenServer.stop(first)

      second = TestUtils.start_writer!(log, bootstrap: config)
      after_restart = Writer.recovered(second)

      assert after_restart.entries == before.entries
    end

    test "writes nothing when it joins", %{tmp_dir: dir} do
      log = Path.join(dir, "log")

      writer = TestUtils.start_writer!(log, join: true)
      opened? = Writer.opened?(writer)

      assert opened? == false
      assert TestUtils.identity_written?(dir) == false
    end

    test "writes its identity when it is opened", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      writer = TestUtils.start_writer!(log, join: true)

      opened = Writer.open(writer)

      assert opened == :ok
      assert TestUtils.identity_written?(dir) == true
    end
  end

  describe "persist/4" do
    test "stops with the sync error when a sync fails", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      disk = DiskStub.new()
      writer = TestUtils.start_writer!(log, disk: disk)
      entries = TestUtils.entries([{1, 1}])
      Process.flag(:trap_exit, true)
      DiskStub.fail_syncs(disk)

      Writer.persist(writer, 1, nil, entries)

      assert_receive {:EXIT, ^writer, {:shutdown, {:sync_failed, :eio}}}, 1_000
    end

    test "does not report a batch whose sync failed", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      disk = DiskStub.new()
      writer = TestUtils.start_writer!(log, disk: disk)
      entries = TestUtils.entries([{1, 1}])
      Process.flag(:trap_exit, true)
      DiskStub.fail_syncs(disk)
      Writer.persist(writer, 1, nil, entries)

      assert_receive {:EXIT, ^writer, _reason}, 1_000

      refute_received {:persisted, _seq}
    end

    test "reports a persist once it is durable", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      writer = TestUtils.start_writer!(log)
      hard_state = TestUtils.hard_state(1, "m1", 0)

      Writer.persist(writer, 1, hard_state, [])

      assert_receive {:persisted, 1}, 1_000
    end

    test "reports one event for requests that arrive together", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      disk = DiskStub.new()
      writer = TestUtils.start_writer!(log, disk: disk)
      before = DiskStub.syncs(disk)

      TestUtils.persist_together!(writer, 64)

      assert_receive {:persisted, 64}, 1_000
      syncs = DiskStub.syncs(disk) - before

      assert syncs == 1
    end

    test "never reports a sequence out of order", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      writer = TestUtils.start_writer!(log)

      TestUtils.persist_each!(writer, 20)
      swapped = TestUtils.persisted_out_of_order(20)

      assert swapped == []
    end

    test "keeps the log in request order", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      writer = TestUtils.start_writer!(log)
      entries = TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])
      replacement = TestUtils.entries([{2, 2}])
      hard_state = TestUtils.hard_state(2, nil, 0)
      disk = DiskStub.new()

      Writer.persist(writer, 1, nil, entries)
      Writer.truncate_from(writer, 2)
      Writer.persist(writer, 2, hard_state, replacement)

      assert_receive {:persisted, 2}, 1_000

      {_store, result} = TestUtils.open_store!(disk, log)
      terms = TestUtils.pluck(result.entries, :term)

      assert terms == [1, 2]
    end

    test "keeps only the last hard state of a batch", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      writer = TestUtils.start_writer!(log)
      older = TestUtils.hard_state(1, "m1", 0)
      newer = TestUtils.hard_state(2, "m2", 0)
      disk = DiskStub.new()

      :sys.suspend(writer)
      Writer.persist(writer, 1, older, [])
      Writer.persist(writer, 2, newer, [])
      :sys.resume(writer)

      assert_receive {:persisted, 2}, 1_000

      {_store, result} = TestUtils.open_store!(disk, log)

      assert %Result{hard_state: %{term: 2, voted_for: "m2"}} = result
    end
  end

  describe "recovered/1" do
    test "returns what the log held at start", %{tmp_dir: dir} do
      log = Path.join(dir, "log")
      writer = TestUtils.start_writer!(log)
      entries = TestUtils.entries([{1, 1}])
      Writer.persist(writer, 1, nil, entries)

      assert_receive {:persisted, 1}, 1_000
      second = TestUtils.start_writer!(log)

      recovered = Writer.recovered(second)

      assert %Result{entries: [%{index: 1}]} = recovered
    end
  end
end
