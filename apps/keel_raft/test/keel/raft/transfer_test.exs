defmodule Keel.Raft.TransferTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.TimeoutNow
  alias Keel.Raft.Node
  alias Keel.TestUtils

  describe "start" do
    test "fails on a follower" do
      node = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      ref = make_ref()

      {_node, effects} = Node.step(node, {:transfer_leadership, ref, "m3"})

      assert effects == [{:reply, ref, {:error, {:not_leader, "m1"}}}]
    end

    test "fails for the leader itself" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {_leader, _ref, effects} = TestUtils.transferring!(leader, "m1")

      assert [{:reply, _ref, {:error, :invalid_target}}] = effects
    end

    test "fails for an unknown member" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {_leader, _ref, effects} = TestUtils.transferring!(leader, "m9")

      assert [{:reply, _ref, {:error, :invalid_target}}] = effects
    end

    test "fails while another transfer runs" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")

      {_leader, _ref, effects} = TestUtils.transferring!(transferring, "m2")

      assert [{:reply, _ref, {:error, :transfer_in_progress}}] = effects
    end

    test "sends timeout now to a target that is up to date" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {_leader, _ref, effects} = TestUtils.transferring!(leader, "m2")

      assert [{:send, %TimeoutNow{to: "m2", term: 1}}] = effects
    end

    test "catches a target up before it sends timeout now" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {_leader, _ref, effects} = TestUtils.transferring!(leader, "m3")

      assert TestUtils.sent_to(effects, TimeoutNow) == []
    end

    test "sends timeout now on the reply that catches the target up" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")

      {_leader, effects} = TestUtils.reply_from!(transferring, "m3", 2)

      assert TestUtils.sent_to(effects, TimeoutNow) == ["m3"]
    end
  end

  describe "propose" do
    test "fails during a transfer" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")
      ref = make_ref()

      {_leader, effects} = Node.step(transferring, {:propose, ref, "x", 0})

      assert effects == [{:reply, ref, {:error, :transfer_in_progress}}]
    end
  end

  describe "tick" do
    test "gives up after one election timeout" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, ref, _effects} = TestUtils.transferring!(leader, "m3")

      {_leader, effects} =
        TestUtils.tick_effects!(transferring, transferring.opts.election_ticks_max)

      assert {:reply, ref, {:error, :timeout}} in effects
    end

    test "accepts proposals again after giving up" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")

      {given_up, _effects} =
        TestUtils.tick_effects!(transferring, transferring.opts.election_ticks_max)

      {_leader, effects} = Node.step(given_up, {:propose, make_ref(), "x", 0})

      assert TestUtils.sent_to(effects, AppendEntries) == ~w[m2 m3]
    end
  end

  describe "step down" do
    test "reports a transfer that the target finished" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, ref, _effects} = TestUtils.transferring!(leader, "m3")
      request = TestUtils.pre_vote_request("m3", "m1", 2, pre_vote?: false, transfer?: true)

      {_follower, effects} = Node.step(transferring, {:message, request})

      assert {:transferred, ref, "m3"} in effects
    end

    test "fails a transfer that another member overtook" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, ref, _effects} = TestUtils.transferring!(leader, "m3")
      request = TestUtils.pre_vote_request("m2", "m1", 2, pre_vote?: false, transfer?: true)

      {_follower, effects} = Node.step(transferring, {:message, request})

      assert {:reply, ref, {:error, {:not_leader, nil}}} in effects
    end

    test "leaves no transfer behind" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")
      higher = TestUtils.heartbeat("m2", "m1", 9)

      {follower, _effects} = Node.step(transferring, {:message, higher})

      assert follower.leader == nil
    end

    test "lets a later leader of the same member propose" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")

      {follower, _effects} =
        Node.step(transferring, {:message, TestUtils.heartbeat("m2", "m1", 9)})

      again = TestUtils.reelected!(follower, "m2")

      {_leader, effects} = Node.step(again, {:propose, make_ref(), "x", 0})

      assert TestUtils.sent_to(effects, AppendEntries) == ~w[m2 m3]
    end
  end

  describe "timeout now" do
    test "makes a follower campaign at once" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      message = TestUtils.timeout_now("m1", "m2", 5)

      {candidate, _effects} = Node.step(follower, {:message, message})

      assert %Node{role: :candidate, term: 6} = candidate
    end

    test "skips the pre-vote round" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      message = TestUtils.timeout_now("m1", "m2", 5)

      {_candidate, effects} = Node.step(follower, {:message, message})

      assert [%RequestVote{pre_vote?: false, transfer?: true}, _other] =
               TestUtils.sent(effects, RequestVote)
    end

    test "is ignored from a member that is not the leader" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      message = TestUtils.timeout_now("m3", "m2", 5)

      {same, effects} = Node.step(follower, {:message, message})

      assert {same.role, effects} == {:follower, []}
    end

    test "is ignored from an older term" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      message = TestUtils.timeout_now("m1", "m2", 4)

      {same, effects} = Node.step(follower, {:message, message})

      assert {same.role, effects} == {:follower, []}
    end
  end
end
