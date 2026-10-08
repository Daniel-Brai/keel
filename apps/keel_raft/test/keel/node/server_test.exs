defmodule Keel.Node.ServerTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.RequestVote
  alias Keel.TestUtils

  @moduletag :tmp_dir
  @moduletag :capture_log

  describe "propose" do
    test "refuses a follower", %{tmp_dir: dir} do
      group = TestUtils.start_slow_member!(dir)

      reply = TestUtils.propose(group, "x")

      assert {:refused, {:not_leader, nil}} = reply
    end

    test "refuses when too many callers wait", %{tmp_dir: dir} do
      group = TestUtils.start_slow_member!(dir, max_pending: 2)
      TestUtils.await_candidate!(group)
      TestUtils.elect!(group)
      _first = TestUtils.propose_async!(group, "1")
      _second = TestUtils.propose_async!(group, "2")
      TestUtils.wait_until!(fn -> TestUtils.waiting(group) == 2 end)

      reply = TestUtils.propose(group, "3")

      assert {:refused, :overloaded} = reply
    end

    test "answers with the machine result once committed", %{tmp_dir: dir} do
      group = TestUtils.start_member!(dir)
      TestUtils.await_role!(group, :leader)

      reply = TestUtils.propose(group, "x")

      assert {:applied, {:ok, {3, _hash}}} = reply
    end
  end

  describe "read" do
    test "refuses a follower without a leader", %{tmp_dir: dir} do
      group = TestUtils.start_slow_member!(dir)

      reply = TestUtils.read(group, :count)

      assert {:refused, {:not_leader, nil}} = reply
    end

    test "answers from the machine once the leader confirms", %{tmp_dir: dir} do
      group = TestUtils.start_member!(dir)
      TestUtils.await_role!(group, :leader)
      {:applied, {:ok, {count, _hash}}} = TestUtils.propose(group, "x")

      reply = TestUtils.read(group, :count)

      assert reply == {:read, {:ok, count}}
    end
  end

  describe "transfer" do
    test "refuses a follower", %{tmp_dir: dir} do
      group = TestUtils.start_slow_member!(dir)

      reply = TestUtils.transfer(group, "b")

      assert {:refused, {:not_leader, nil}} = reply
    end

    test "refuses an unknown target", %{tmp_dir: dir} do
      group = TestUtils.start_member!(dir)
      TestUtils.await_role!(group, :leader)

      reply = TestUtils.transfer(group, "zz")

      assert reply == {:refused, :invalid_target}
    end
  end

  describe "messages" do
    test "drops a message of another cluster", %{tmp_dir: dir} do
      group = TestUtils.start_slow_member!(dir)
      message = TestUtils.request_vote("b", "a", 1)

      TestUtils.send_to_server(group, {:keel, <<9::128>>, message})
      TestUtils.send_to_server(group, {:keel, TestUtils.cluster_id(), message})

      TestUtils.wait_until!(fn -> TestUtils.status(group).term == 1 end)

      status = TestUtils.status(group)

      assert status.dropped == 1
    end

    test "drops a message for another member", %{tmp_dir: dir} do
      group = TestUtils.start_slow_member!(dir)
      message = TestUtils.request_vote("b", "c", 1)

      TestUtils.send_to_server(group, {:keel, TestUtils.cluster_id(), message})

      TestUtils.wait_until!(fn -> TestUtils.status(group).dropped == 1 end)

      status = TestUtils.status(group)

      assert status.term == 0
    end
  end

  describe "a joining member" do
    test "writes nothing before a leader speaks", %{tmp_dir: dir} do
      _group = TestUtils.start_joining_member!(dir)

      refute_receive {:sent, _to, _message}

      assert TestUtils.identity_written?(dir) == false
    end

    test "drops a vote request and writes nothing", %{tmp_dir: dir} do
      group = TestUtils.start_joining_member!(dir)
      message = TestUtils.request_vote("b", "a", 1)

      TestUtils.send_to_server(group, {:keel, TestUtils.cluster_id(), message})
      TestUtils.wait_until!(fn -> TestUtils.status(group).dropped == 1 end)

      status = TestUtils.status(group)

      assert status.term == 0
      assert TestUtils.identity_written?(dir) == false
    end

    test "drops an append of another cluster", %{tmp_dir: dir} do
      group = TestUtils.start_joining_member!(dir)
      message = TestUtils.heartbeat("b", "a", 1)

      TestUtils.send_to_server(group, {:keel, <<9::128>>, message})
      TestUtils.wait_until!(fn -> TestUtils.status(group).dropped == 1 end)

      assert TestUtils.identity_written?(dir) == false
    end

    test "writes its identity when the first append arrives", %{tmp_dir: dir} do
      group = TestUtils.start_joining_member!(dir)
      message = TestUtils.heartbeat("b", "a", 1)

      TestUtils.send_to_server(group, {:keel, TestUtils.cluster_id(), message})
      TestUtils.wait_until!(fn -> TestUtils.status(group).term == 1 end)

      assert TestUtils.identity_written?(dir) == true
    end

    test "never starts an election", %{tmp_dir: dir} do
      group = TestUtils.start_joining_member!(dir)
      message = TestUtils.heartbeat("b", "a", 1)

      TestUtils.send_to_server(group, {:keel, TestUtils.cluster_id(), message})
      TestUtils.wait_until!(fn -> TestUtils.status(group).term == 1 end)

      refute_receive {:sent, _to, %RequestVote{}}

      status = TestUtils.status(group)

      assert status.role == :follower
    end
  end

  describe "ticks" do
    test "asks both peers for votes once the timer runs out", %{tmp_dir: dir} do
      TestUtils.start_slow_member!(dir)

      assert_receive {:sent, "b", %RequestVote{from: "a"}}
      assert_receive {:sent, "c", %RequestVote{from: "a"}}
    end

    test "leads after one vote and sends entries", %{tmp_dir: dir} do
      group = TestUtils.start_slow_member!(dir)
      TestUtils.await_candidate!(group)

      TestUtils.elect!(group)

      assert_receive {:sent, "b", %AppendEntries{from: "a"}}
    end
  end
end
