defmodule Keel.Transport.FaultProxyTest do
  use ExUnit.Case, async: true

  alias Keel.TestUtils
  alias Keel.Transport.FaultProxy

  describe "send/3" do
    test "drops what a block rule names" do
      group = TestUtils.start_proxy!()
      message = TestUtils.request_vote("a", "b", 1)
      FaultProxy.block(group, "b")

      FaultProxy.send(group, "b", message)

      refute_receive {:sent, "b", _message}
    end

    test "drops everything with a drop chance of one" do
      group = TestUtils.start_proxy!()
      message = TestUtils.request_vote("a", "b", 1)
      FaultProxy.set(group, "b", {:drop, 1.0})

      FaultProxy.send(group, "b", message)

      refute_receive {:sent, "b", _message}
    end

    test "still delivers to members the rule does not name" do
      group = TestUtils.start_proxy!()
      message = TestUtils.request_vote("a", "c", 1)
      FaultProxy.block(group, "b")

      FaultProxy.send(group, "c", message)

      assert_receive {:sent, "c", ^message}
    end

    test "delivers once a block rule is lifted" do
      group = TestUtils.start_proxy!()
      message = TestUtils.request_vote("a", "b", 1)
      FaultProxy.block(group, "b")
      FaultProxy.unblock(group, "b")

      FaultProxy.send(group, "b", message)

      assert_receive {:sent, "b", ^message}
    end

    test "delivers twice with a duplicate chance of one" do
      group = TestUtils.start_proxy!()
      message = TestUtils.request_vote("a", "b", 1)
      FaultProxy.set(group, "b", {:duplicate, 1.0})

      FaultProxy.send(group, "b", message)

      assert_receive {:sent, "b", ^message}
      assert_receive {:sent, "b", ^message}
    end

    test "lets a delayed message be overtaken" do
      group = TestUtils.start_proxy!()
      slow = TestUtils.request_vote("a", "b", 1)
      quick = TestUtils.request_vote("a", "c", 1)
      FaultProxy.set(group, "b", {:delay, 50, 50})

      FaultProxy.send(group, "b", slow)
      FaultProxy.send(group, "c", quick)
      first = TestUtils.first_of_two!()

      assert {:sent, "c", ^quick} = first
    end
  end

  describe "partition/2" do
    test "blocks every named member until cleared" do
      group = TestUtils.start_proxy!()
      message = TestUtils.request_vote("a", "b", 1)
      FaultProxy.partition(group, ~w[b c])

      FaultProxy.send(group, "b", message)

      refute_receive {:sent, "b", _message}

      FaultProxy.clear(group)
      FaultProxy.send(group, "b", message)

      assert_receive {:sent, "b", ^message}
    end
  end
end
