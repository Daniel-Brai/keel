defmodule Keel.Raft.ReadsTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.ReadIndex
  alias Keel.Raft.Message.ReadIndexReply
  alias Keel.Raft.Node
  alias Keel.TestUtils

  describe "a leader that removed itself" do
    test "does not answer on one of the other two voters" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {removing, _ref, _effects} = TestUtils.change!(leader, {:remove, "m1"})
      {reading, _refs, _effects} = TestUtils.read!(removing, 1)

      {_leader, effects} = TestUtils.reply_from!(reading, "m3", 3)

      assert effects == []
    end

    test "answers once both other voters replied" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {removing, _ref, _effects} = TestUtils.change!(leader, {:remove, "m1"})
      {reading, [ref], _effects} = TestUtils.read!(removing, 1)
      {waiting, _effects} = TestUtils.reply_from!(reading, "m3", 3)

      {_leader, effects} = TestUtils.reply_from!(waiting, "m2", 3)

      assert [{:read_ready, ^ref, 1} | _rest] = effects
    end
  end

  describe "request" do
    test "fails on a follower without a leader" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      ref = make_ref()

      {_node, effects} = Node.step(node, {:read_index, ref})

      assert effects == [{:reply, ref, {:error, {:not_leader, nil}}}]
    end

    test "fails during a transfer" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")
      ref = make_ref()

      {_node, effects} = Node.step(transferring, {:read_index, ref})

      assert effects == [{:reply, ref, {:error, :transfer_in_progress}}]
    end

    test "waits for the no-op to commit" do
      leader = TestUtils.leader!("m1", ~w[m1 m2 m3])

      {_leader, _refs, effects} = TestUtils.read!(leader, 1)

      assert effects == []
    end

    test "answers at once in a single voter cluster" do
      leader = TestUtils.single_voter_leader!("m1")
      ref = make_ref()

      {_leader, effects} = Node.step(leader, {:read_index, ref})

      assert effects == [{:read_ready, ref, 1}]
    end

    test "sends one heartbeat round to the other voters" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {_leader, _refs, effects} = TestUtils.read!(leader, 1)

      assert TestUtils.sent_to(effects, AppendEntries) == ~w[m2 m3]
    end

    test "answers after a quorum replied" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {reading, [ref], _effects} = TestUtils.read!(leader, 1)

      {_leader, effects} = TestUtils.reply_from!(reading, "m3", 2)

      assert effects == [{:read_ready, ref, 1}]
    end

    test "does not answer on a reply to an older heartbeat" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {reading, _refs, _effects} = TestUtils.read!(leader, 1)

      {_leader, effects} = TestUtils.reply_from!(reading, "m3", 1)

      assert effects == []
    end

    test "answers on a rejection from the current term" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {reading, [ref], _effects} = TestUtils.read!(leader, 1)

      {_leader, effects} = TestUtils.reply_from!(reading, "m3", 2, false)

      assert {:read_ready, ref, 1} in effects
    end

    test "starts no round for a read queued behind one" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {reading, _refs, _effects} = TestUtils.read!(leader, 1)

      {_leader, _refs, effects} = TestUtils.read!(reading, 1)

      assert effects == []
    end

    test "answers queued reads together after one more round" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {reading, [_first | queued], _effects} = TestUtils.read!(leader, 3)
      {next, _effects} = TestUtils.reply_from!(reading, "m3", 2)

      {_leader, effects} = TestUtils.reply_from!(next, "m3", 3)

      assert TestUtils.read_refs(effects) == queued
    end

    test "fails the reads of a leader that steps down" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {reading, [ref], _effects} = TestUtils.read!(leader, 1)
      higher = TestUtils.heartbeat("m2", "m1", 9)

      {_follower, effects} = Node.step(reading, {:message, higher})

      assert {:reply, ref, {:error, {:not_leader, nil}}} in effects
    end

    test "fails reads that never get a quorum" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {reading, [ref], _effects} = TestUtils.read!(leader, 1)

      {_leader, effects} = TestUtils.tick_effects!(reading, reading.opts.election_ticks_max)

      assert {:reply, ref, {:error, :not_ready}} in effects
    end
  end

  describe "confirming" do
    test "sends an empty append at each follower's match index" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      matched = leader.log.last_index
      {proposed, _effects} = Node.step(leader, {:propose, make_ref(), "x", 0})

      {_reading, _refs, effects} = TestUtils.read!(proposed, 1)
      appends = TestUtils.sent_to_peer(effects, AppendEntries, "m2")

      assert [%AppendEntries{prev_index: ^matched, entries: []}] = appends
    end

    test "leaves the next index of a pipelining follower alone" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {proposed, _effects} = Node.step(leader, {:propose, make_ref(), "x", 0})
      next = proposed.leader.progress["m2"].next_index

      {reading, _refs, _effects} = TestUtils.read!(proposed, 1)

      assert reading.leader.progress["m2"].next_index == next
    end
  end

  describe "forwarding" do
    test "sends the leader a read index request" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      ref = make_ref()

      {_follower, effects} = Node.step(follower, {:read_index, ref})

      assert [{:send, %ReadIndex{to: "m1", ref_id: 1, term: 5}}] = effects
    end

    test "gives each forward its own number" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      {forwarded, _effects} = Node.step(follower, {:read_index, make_ref()})

      {_follower, effects} = Node.step(forwarded, {:read_index, make_ref()})

      assert [{:send, %ReadIndex{ref_id: 2}}] = effects
    end

    test "fails a forward that waits too long" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      ref = make_ref()
      {forwarded, _effects} = Node.step(follower, {:read_index, ref})

      {_follower, effects} = TestUtils.tick_effects!(forwarded, forwarded.opts.election_ticks_min)

      assert {:reply, ref, {:error, :not_ready}} in effects
    end

    test "answers the client with the index of the leader" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      ref = make_ref()
      {forwarded, _effects} = Node.step(follower, {:read_index, ref})
      reply = TestUtils.read_reply("m2", 1, 5, 7, true)

      {_follower, effects} = Node.step(forwarded, {:message, reply})

      assert effects == [{:read_ready, ref, 7}]
    end

    test "fails the client when the leader refuses" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      ref = make_ref()
      {forwarded, _effects} = Node.step(follower, {:read_index, ref})
      reply = TestUtils.read_reply("m2", 1, 5, 0, false)

      {_follower, effects} = Node.step(forwarded, {:message, reply})

      assert effects == [{:reply, ref, {:error, :not_ready}}]
    end

    test "fails the forwards when the leader changes" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      ref = make_ref()
      {forwarded, _effects} = Node.step(follower, {:read_index, ref})
      append = TestUtils.heartbeat("m3", "m2", 6)

      {_follower, effects} = Node.step(forwarded, {:message, append})

      assert {:reply, ref, {:error, {:not_leader, nil}}} in effects
    end

    test "keeps the forwards while the leader stays" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      {forwarded, _effects} = Node.step(follower, {:read_index, make_ref()})
      append = TestUtils.heartbeat("m1", "m2", 5)

      {kept, _effects} = Node.step(forwarded, {:message, append})

      assert map_size(kept.forwards) == 1
    end

    test "confirms a forwarded read for the follower" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      request = TestUtils.forwarded_read("m2", 4, leader.term)
      {reading, _effects} = Node.step(leader, {:message, request})

      {_leader, effects} = TestUtils.reply_from!(reading, "m3", 2)

      assert [{:send, %ReadIndexReply{to: "m2", ref_id: 4, index: 1, ok?: true}}] = effects
    end

    test "refuses a forwarded read on a follower" do
      node = TestUtils.following!("m1", ~w[m1 m2 m3], 5, "m2")
      request = TestUtils.forwarded_read("m3", 4, 5)

      {_node, effects} = Node.step(node, {:message, request})

      assert [{:send, %ReadIndexReply{to: "m3", ref_id: 4, ok?: false}}] = effects
    end

    test "refuses a forwarded read during a transfer" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")
      request = TestUtils.forwarded_read("m2", 4, leader.term)

      {_node, effects} = Node.step(transferring, {:message, request})

      assert [{:send, %ReadIndexReply{ok?: false}}] = effects
    end

    test "refuses the forwarded reads of a leader that steps down" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      request = TestUtils.forwarded_read("m2", 4, leader.term)
      {reading, _effects} = Node.step(leader, {:message, request})
      higher = TestUtils.heartbeat("m3", "m1", 9)

      {_follower, effects} = Node.step(reading, {:message, higher})

      assert [%ReadIndexReply{to: "m2", ref_id: 4, ok?: false}] =
               TestUtils.sent(effects, ReadIndexReply)
    end
  end
end
