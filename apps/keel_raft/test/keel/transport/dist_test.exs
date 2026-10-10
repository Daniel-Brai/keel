defmodule Keel.Transport.DistTest do
  use ExUnit.Case, async: true

  alias Keel.Common.Name
  alias Keel.TestUtils
  alias Keel.Transport.Dist

  describe "update_members/2" do
    test "stops the sender of a member that is gone" do
      group = TestUtils.random_group()
      TestUtils.start_transport!(group, Name.build("dest", group))
      message = TestUtils.request_vote("a", "b", 1)

      :ok = Dist.update_members(group, %{"c" => node()})
      Dist.send(group, "b", message)

      refute_receive {:keel, _cluster, _message}
    end

    test "starts the sender of a member that joined" do
      group = TestUtils.random_group()
      TestUtils.start_transport!(group, Name.build("dest", group))
      message = TestUtils.request_vote("a", "c", 1)

      :ok = Dist.update_members(group, %{"b" => node(), "c" => node()})
      Dist.send(group, "c", message)
      cluster_id = TestUtils.cluster_id()

      assert_receive {:keel, ^cluster_id, ^message}
    end
  end

  describe "send/3" do
    test "drops a message for an unknown member" do
      group = TestUtils.random_group()
      TestUtils.start_transport!(group, Name.build("dest", group))
      message = TestUtils.request_vote("a", "z", 1)

      sent = Dist.send(group, "z", message)

      assert sent == :ok
      refute_receive {:keel, _cluster, _message}
    end

    test "wraps the message with the cluster id" do
      group = TestUtils.random_group()
      TestUtils.start_transport!(group, Name.build("dest", group))
      message = TestUtils.request_vote("a", "b", 1)
      cluster_id = TestUtils.cluster_id()

      Dist.send(group, "b", message)

      assert_receive {:keel, ^cluster_id, ^message}
    end

    test "keeps the order of messages to one member" do
      group = TestUtils.random_group()
      TestUtils.start_transport!(group, Name.build("dest", group))
      messages = Enum.map(1..20, &TestUtils.request_vote("a", "b", &1))

      Enum.each(messages, &Dist.send(group, "b", &1))
      terms = TestUtils.received_terms(20)

      expected = Enum.to_list(1..20)

      assert terms == expected
    end
  end
end
