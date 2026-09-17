defmodule Keel.Raft.ReplicationTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.InvariantError
  alias Keel.Raft.Log
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Node
  alias Keel.Raft.Replication
  alias Keel.TestUtils

  describe "propose" do
    test "fails on a follower" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      ref = make_ref()

      {_follower, effects} = Node.step(node, {:propose, ref, "x", 0})

      assert effects == [{:reply, ref, {:error, {:not_leader, nil}}}]
    end

    test "answers pending proposals when leadership ends" do
      leader = TestUtils.durable_leader!("m1", ~w[m1 m2 m3])
      ref = make_ref()
      {proposed, _effects} = Node.step(leader, {:propose, ref, "x", 0})

      reply = %AppendEntriesReply{
        term: 9,
        from: "m2",
        to: "m1",
        success?: false,
        match_index: 0,
        seq: 1
      }

      {_follower, effects} = Node.step(proposed, {:message, reply})

      assert {:reply, ref, {:error, :leadership_lost}} in effects
    end
  end

  describe "entry time" do
    test "stamps a proposal with the time it carries" do
      leader = TestUtils.durable_leader!("m1", ~w[m1 m2 m3])
      ref = make_ref()

      {proposed, _effects} = Node.step(leader, {:propose, ref, "x", 500})

      assert proposed.log.last_time_ms == 500
    end

    test "never lets log time go back" do
      leader = TestUtils.durable_leader!("m1", ~w[m1 m2 m3])
      one = make_ref()
      two = make_ref()
      {first, _effects} = Node.step(leader, {:propose, one, "x", 500})

      {second, _effects} = Node.step(first, {:propose, two, "y", 100})
      last_time = Log.time_at(second.log, second.log.last_index)

      assert last_time == 500
    end

    test "gives the no-op the time of the last entry" do
      held = TestUtils.entries([{1, 1}, {2, 1}])

      leader = TestUtils.leader_with_log!("m1", held)

      assert leader.log.last_time_ms == 20
    end

    test "restores the time of the last kept entry after a truncation" do
      held = TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])
      log = Log.new(held)

      truncated = Log.truncate_from(log, 3)

      assert truncated.last_time_ms == 20
    end
  end

  describe "append" do
    test "rejects entries whose previous entry it lacks" do
      held = TestUtils.entries([{1, 1}])
      new = TestUtils.entries([{4, 2}])
      node = TestUtils.follower_with_log!("m2", 2, held)
      append = TestUtils.append(2, 3, 2, new, 0)

      {_follower, effects} = Node.step(node, {:message, append})

      replies = TestUtils.sent(effects, AppendEntriesReply)

      assert [%AppendEntriesReply{success?: false, match_index: 1}] = replies
    end

    test "rejects entries whose previous term differs" do
      held = TestUtils.entries([{1, 1}])
      new = TestUtils.entries([{2, 2}])
      node = TestUtils.follower_with_log!("m2", 2, held)
      append = TestUtils.append(2, 1, 2, new, 0)

      {_follower, effects} = Node.step(node, {:message, append})
      replies = TestUtils.sent(effects, AppendEntriesReply)

      assert [%AppendEntriesReply{success?: false}] = replies
    end

    test "holds the ack until the entries are durable" do
      node = TestUtils.follower_with_log!("m2", 2, [])
      new = TestUtils.entries([{1, 2}])
      append = TestUtils.append(2, 0, 0, new, 0)

      {_follower, effects} = Node.step(node, {:message, append})

      targets = TestUtils.sent_to(effects, AppendEntriesReply)

      assert targets == []
    end

    test "drops a held ack for entries a newer leader replaced" do
      node = TestUtils.follower_with_log!("m2", 1, [])
      old = TestUtils.entries([{1, 1}, {2, 1}])
      first = TestUtils.append(1, 0, 0, old, 0)
      {held, _effects} = Node.step(node, {:message, first})
      new = TestUtils.entries([{2, 2}])
      second = TestUtils.append(2, 1, 1, new, 0)
      {replaced, _effects} = Node.step(held, {:message, second})

      {_released, effects} = Node.step(replaced, {:persisted, held.persist_seq})
      replies = TestUtils.sent(effects, AppendEntriesReply)

      assert replies == []
    end

    test "holds an ack until its term is durable" do
      held = TestUtils.entries([{1, 1}, {2, 1}])
      node = TestUtils.follower_with_log!("m2", 1, held)
      heartbeat = TestUtils.append(2, 2, 1, [], 0)

      {_follower, effects} = Node.step(node, {:message, heartbeat})
      replies = TestUtils.sent(effects, AppendEntriesReply)

      assert replies == []
    end

    test "acks entries already on disk while a later write is pending" do
      held = TestUtils.entries([{1, 1}, {2, 1}])
      node = TestUtils.follower_with_log!("m2", 1, held)
      new = TestUtils.entries([{3, 1}])
      writing = TestUtils.append(1, 2, 1, new, 0)
      {pending, _effects} = Node.step(node, {:message, writing})
      heartbeat = TestUtils.append(1, 2, 1, [], 0)

      {_follower, effects} = Node.step(pending, {:message, heartbeat})
      replies = TestUtils.sent(effects, AppendEntriesReply)

      assert [%AppendEntriesReply{success?: true, match_index: 2}] = replies
    end

    test "acks once the entries are durable" do
      node = TestUtils.follower_with_log!("m2", 2, [])
      new = TestUtils.entries([{1, 2}])
      append = TestUtils.append(2, 0, 0, new, 0)
      {follower, _effects} = Node.step(node, {:message, append})

      {_durable, effects} = Node.step(follower, {:persisted, follower.persist_seq})

      assert [{:send, %AppendEntriesReply{success?: true, match_index: 1}}] = effects
    end

    test "skips entries it already holds" do
      entries = TestUtils.entries([{1, 1}, {2, 1}])
      node = TestUtils.follower_with_log!("m2", 1, entries)
      append = TestUtils.append(1, 0, 0, entries, 0)

      {_follower, effects} = Node.step(node, {:message, append})

      found? = Enum.any?(effects, &match?({:truncate_from, _index}, &1))

      refute found?
    end

    test "truncates a conflicting suffix" do
      held = TestUtils.entries([{1, 1}, {2, 1}])
      new = TestUtils.entries([{2, 2}])
      node = TestUtils.follower_with_log!("m2", 2, held)
      append = TestUtils.append(2, 1, 1, new, 0)

      {follower, effects} = Node.step(node, {:message, append})
      term = Log.term_at(follower.log, 2)

      assert {:truncate_from, 2} in effects
      assert term == 2
    end

    test "raises before truncating a committed entry" do
      held = TestUtils.entries([{1, 1}, {2, 1}])
      new = TestUtils.entries([{2, 2}])
      node = TestUtils.follower_with_log!("m2", 2, held)
      committed = %{node | commit: 2}
      append = TestUtils.append(2, 1, 1, new, 2)

      assert_raise InvariantError, fn -> Node.step(committed, {:message, append}) end
    end

    test "commits no further than the last new entry" do
      held = TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])
      new = TestUtils.entries([{1, 1}])
      node = TestUtils.follower_with_log!("m2", 1, held)
      append = TestUtils.append(1, 0, 0, new, 3)

      {follower, _effects} = Node.step(node, {:message, append})

      assert follower.commit == 1
    end
  end

  describe "commit" do
    test "does not count an older term entry alone" do
      leader = TestUtils.durable_leader!("m1", ~w[m1 m2 m3])
      log = [{1, 0}] |> TestUtils.entries() |> Log.new()
      older = %{leader | log: log, durable_index: 1}

      {after_ack, _effects} = TestUtils.ack(older, "m2", 1)

      assert after_ack.commit == 0
    end

    test "commits an entry of its own term held by a quorum" do
      leader = TestUtils.durable_leader!("m1", ~w[m1 m2 m3])

      {committed, effects} = TestUtils.ack(leader, "m2", leader.log.last_index)

      assert committed.commit == leader.log.last_index
      assert [{:apply, [_noop], %{}} | _rest] = effects
    end
  end

  describe "pipeline" do
    test "stops after a refusal" do
      leader = TestUtils.pipelining_leader!()
      refusal = TestUtils.failed_reply(leader, "m2", 30)

      {refused, _effects} = Node.step(leader, {:message, refusal})

      refute refused.leader.progress["m2"].pipelining?
    end

    test "sends only an empty probe while the window is full" do
      leader = TestUtils.pipelining_leader!(max_append_entries: 2, max_inflight_appends: 1)
      {proposed, proposed_effects} = Node.step(leader, {:propose, make_ref(), "x", 0})

      appends = TestUtils.sent_to_peer(proposed_effects, AppendEntries, "m2")

      assert appends == []

      {_probed, probe_effects} = Replication.probe(proposed, "m2")
      probes = TestUtils.sent_to_peer(probe_effects, AppendEntries, "m2")

      assert [%AppendEntries{prev_index: 28, entries: []}] = probes
    end

    test "sends the next batch as soon as a behind follower acks" do
      entries = TestUtils.run_entries(1, 40, 1)
      leader = TestUtils.leader_with_log!("m1", entries)
      refusal = TestUtils.failed_reply(leader, "m2", 10)
      {probed, _effects} = Node.step(leader, {:message, refusal})

      {_acked, effects} = TestUtils.ack(probed, "m2", 26)
      appends = TestUtils.sent_to_peer(effects, AppendEntries, "m2")

      assert [%AppendEntries{prev_index: 26}] = appends
    end

    test "sends only new entries once the follower acks" do
      leader = TestUtils.pipelining_leader!()

      {_proposed, effects} = Node.step(leader, {:propose, make_ref(), "x", 0})
      appends = TestUtils.sent_to_peer(effects, AppendEntries, "m2")

      assert [%AppendEntries{prev_index: 41, entries: [_new]}] = appends
    end
  end
end
