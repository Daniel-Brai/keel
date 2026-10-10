defmodule Keel.Transport.EnvelopeTest do
  use ExUnit.Case, async: true

  alias Keel.TestUtils
  alias Keel.Transport.Envelope

  describe "check/4" do
    test "refuses another cluster" do
      message = TestUtils.request_vote("m1", "m2", 1)

      checked = Envelope.check(message, <<1::128>>, <<2::128>>, "m2")

      assert checked == {:error, :invalid}
    end

    test "refuses a message for another member" do
      message = TestUtils.request_vote("m1", "m3", 1)

      checked = Envelope.check(message, <<1::128>>, <<1::128>>, "m2")

      assert checked == {:error, :invalid}
    end

    test "refuses a term that is not a message" do
      checked = Envelope.check({:evil, "m2"}, <<1::128>>, <<1::128>>, "m2")

      assert checked == {:error, :invalid}
    end

    test "accepts a message of this cluster for this member" do
      message = TestUtils.request_vote("m1", "m2", 1)

      checked = Envelope.check(message, <<1::128>>, <<1::128>>, "m2")

      assert checked == :ok
    end
  end

  describe "check/5" do
    test "refuses an append from a removed member" do
      message = TestUtils.heartbeat("m1", "m2", 1)

      checked = Envelope.check(message, <<1::128>>, <<1::128>>, "m2", MapSet.new(["m1"]))

      assert checked == {:error, :invalid}
    end

    test "accepts a vote request from a removed member" do
      message = TestUtils.request_vote("m1", "m2", 1)

      checked = Envelope.check(message, <<1::128>>, <<1::128>>, "m2", MapSet.new(["m1"]))

      assert checked == :ok
    end

    test "accepts a reply from a removed member" do
      message = TestUtils.append_entries_reply("m1", "m2", 1)

      checked = Envelope.check(message, <<1::128>>, <<1::128>>, "m2", MapSet.new(["m1"]))

      assert checked == :ok
    end

    test "accepts an append from a member that is not removed" do
      message = TestUtils.heartbeat("m1", "m2", 1)

      checked = Envelope.check(message, <<1::128>>, <<1::128>>, "m2", MapSet.new(["m3"]))

      assert checked == :ok
    end
  end

  describe "check_join/4" do
    test "refuses a vote request" do
      message = TestUtils.request_vote("m1", "m2", 1)

      checked = Envelope.check_join(message, <<1::128>>, <<1::128>>, "m2")

      assert checked == {:error, :invalid}
    end

    test "refuses an append of another cluster" do
      message = TestUtils.heartbeat("m1", "m2", 1)

      checked = Envelope.check_join(message, <<2::128>>, <<1::128>>, "m2")

      assert checked == {:error, :invalid}
    end

    test "accepts an append of its cluster" do
      message = TestUtils.heartbeat("m1", "m2", 1)

      checked = Envelope.check_join(message, <<1::128>>, <<1::128>>, "m2")

      assert checked == :ok
    end

    test "accepts a snapshot chunk of its cluster" do
      meta = TestUtils.snapshot_meta(5, 1)
      message = TestUtils.install(meta, 0, 16)

      checked = Envelope.check_join(message, <<1::128>>, <<1::128>>, "m2")

      assert checked == :ok
    end
  end

  describe "wrap/2" do
    test "names the cluster" do
      message = TestUtils.request_vote("m1", "m2", 1)

      wrapped = Envelope.wrap(<<1::128>>, message)

      assert {:keel, <<1::128>>, ^message} = wrapped
    end
  end
end
