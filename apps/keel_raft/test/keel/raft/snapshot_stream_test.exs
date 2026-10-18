defmodule Keel.Raft.SnapshotStreamTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.SnapshotStream
  alias Keel.TestUtils

  describe "pull/1" do
    test "sends one empty chunk for an empty file" do
      stream = TestUtils.snapshot_stream(0, 4)

      {_pulled, ranges} = SnapshotStream.pull(stream)

      assert ranges == [{0, 0}]
    end

    test "never has more chunks out than the window allows" do
      stream = TestUtils.snapshot_stream(100, 2)

      {_pulled, ranges} = SnapshotStream.pull(stream)

      assert ranges == [{0, 16}, {16, 16}]
    end

    test "stops at the end of the file with a short last chunk" do
      stream = TestUtils.snapshot_stream(20, 4)

      {_pulled, ranges} = SnapshotStream.pull(stream)

      assert ranges == [{0, 16}, {16, 4}]
    end
  end

  describe "ack/2" do
    test "opens the window for the next chunk" do
      stream = TestUtils.snapshot_stream(100, 2)
      {pulled, _ranges} = SnapshotStream.pull(stream)

      {_next, ranges} = pulled |> SnapshotStream.ack(16) |> SnapshotStream.pull()

      assert ranges == [{32, 16}]
    end

    test "resumes from a lower offset" do
      stream = TestUtils.snapshot_stream(100, 2)
      {pulled, _ranges} = SnapshotStream.pull(stream)
      acked = SnapshotStream.ack(pulled, 32)

      {_next, ranges} = acked |> SnapshotStream.ack(16) |> SnapshotStream.pull()

      assert ranges == [{16, 16}, {32, 16}]
    end
  end

  describe "complete?/1" do
    test "is true once every byte is acknowledged" do
      stream = TestUtils.snapshot_stream(32, 4)
      {pulled, _ranges} = SnapshotStream.pull(stream)

      acked = SnapshotStream.ack(pulled, 32)

      assert SnapshotStream.complete?(acked)
    end
  end

  describe "message/3" do
    test "marks the chunk that reaches the end as done" do
      stream = TestUtils.snapshot_stream(20, 4)

      message = SnapshotStream.message(stream, 16, TestUtils.random_chunk(4))

      assert {message.done?, message.seq} == {true, 9}
    end
  end
end
