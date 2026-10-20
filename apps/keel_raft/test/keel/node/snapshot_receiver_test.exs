defmodule Keel.Node.SnapshotReceiverTest do
  use ExUnit.Case, async: true

  alias Keel.Env.Disk.Posix
  alias Keel.Log.Snapshot
  alias Keel.Node.SnapshotReceiver
  alias Keel.TestUtils

  @moduletag :tmp_dir
  @moduletag :capture_log

  describe "chunk/5" do
    test "reports nothing for a file with the wrong checksum", %{tmp_dir: dir} do
      receiver = TestUtils.start_receiver!(dir)
      payload = String.duplicate("s", 100)
      {bytes, meta} = TestUtils.snapshot_bytes(5, 1, payload)
      wrong = %{meta | checksum: meta.checksum + 1}

      TestUtils.send_chunks!(receiver, wrong, bytes, 40)

      refute_received {:snapshot_received, _meta}
    end

    test "removes the part file after a wrong checksum", %{tmp_dir: dir} do
      receiver = TestUtils.start_receiver!(dir)
      payload = String.duplicate("s", 100)
      {bytes, meta} = TestUtils.snapshot_bytes(5, 1, payload)
      wrong = %{meta | checksum: meta.checksum + 1}

      TestUtils.send_chunks!(receiver, wrong, bytes, 40)

      snap = TestUtils.snap_dir(dir)
      tmp = Snapshot.tmp_dir(snap)

      assert TestUtils.snapshot_files(tmp) == []
    end

    test "ignores a chunk that continues nothing", %{tmp_dir: dir} do
      receiver = TestUtils.start_receiver!(dir)
      {_bytes, meta} = TestUtils.snapshot_bytes(5, 1, "state")

      SnapshotReceiver.chunk(receiver, meta, 40, "late", true)
      :sys.get_state(receiver)

      refute_received {:snapshot_received, _meta}
    end

    test "reports the snapshot once its file is in place", %{tmp_dir: dir} do
      receiver = TestUtils.start_receiver!(dir)
      payload = String.duplicate("s", 100)
      {bytes, meta} = TestUtils.snapshot_bytes(5, 1, payload)

      TestUtils.send_chunks!(receiver, meta, bytes, 40)

      assert_received {:snapshot_received, ^meta}
    end

    test "leaves a file that reads back", %{tmp_dir: dir} do
      receiver = TestUtils.start_receiver!(dir)
      {bytes, meta} = TestUtils.snapshot_bytes(5, 1, "machine state")

      TestUtils.send_chunks!(receiver, meta, bytes, 16)

      snap = TestUtils.snap_dir(dir)
      read = Snapshot.read(Posix.new(), snap, meta)

      assert {:ok, %Posix{}, "machine state"} = read
    end
  end
end
