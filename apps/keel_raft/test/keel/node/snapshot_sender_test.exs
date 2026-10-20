defmodule Keel.Node.SnapshotSenderTest do
  use ExUnit.Case, async: true

  alias Keel.TestUtils

  @moduletag :tmp_dir
  @moduletag :capture_log

  describe "ack/2" do
    test "sends no more chunks than the window allows", %{tmp_dir: dir} do
      snap = TestUtils.snap_dir(dir)
      payload = String.duplicate("s", 150)
      meta = TestUtils.write_snapshot!(snap, 5, 1, payload)

      _sender = TestUtils.start_sender!(dir, meta)

      assert TestUtils.sent_offsets() == [0, 32]
    end

    test "sends the next chunk once one is acknowledged", %{tmp_dir: dir} do
      snap = TestUtils.snap_dir(dir)
      payload = String.duplicate("s", 150)
      meta = TestUtils.write_snapshot!(snap, 5, 1, payload)
      sender = TestUtils.start_sender!(dir, meta)
      TestUtils.sent_offsets()

      TestUtils.ack_sender!(sender, 32)

      assert TestUtils.sent_offsets() == [64]
    end

    test "resumes from a lower acknowledged offset", %{tmp_dir: dir} do
      snap = TestUtils.snap_dir(dir)
      payload = String.duplicate("s", 150)
      meta = TestUtils.write_snapshot!(snap, 5, 1, payload)
      sender = TestUtils.start_sender!(dir, meta)
      TestUtils.ack_sender!(sender, 64)
      TestUtils.sent_offsets()

      TestUtils.ack_sender!(sender, 32)

      assert TestUtils.sent_offsets() == [32, 64]
    end
  end

  describe "start_link/1" do
    test "starts again from zero when no acknowledgement comes", %{tmp_dir: dir} do
      snap = TestUtils.snap_dir(dir)
      payload = String.duplicate("s", 150)
      meta = TestUtils.write_snapshot!(snap, 5, 1, payload)

      _sender = TestUtils.start_sender!(dir, meta, ack_timeout_ms: 20)

      assert TestUtils.await_offsets(4) == [0, 32, 0, 32]
    end
  end
end
