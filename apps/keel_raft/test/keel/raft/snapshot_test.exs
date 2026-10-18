defmodule Keel.Raft.SnapshotTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.Log
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.InstallSnapshotReply
  alias Keel.Raft.Node
  alias Keel.TestUtils

  describe "install" do
    test "fails for a stale term" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 3)
      meta = TestUtils.snapshot_meta(5, 1)
      stale = TestUtils.install(meta, 0, 512, term: 2)

      {refused, effects} = Node.step(node, {:message, stale})

      assert {refused.election_elapsed, effects} ==
               {node.election_elapsed,
                [
                  {:send,
                   %InstallSnapshotReply{
                     term: 3,
                     from: "m2",
                     to: "m1",
                     offset: 0,
                     done?: false,
                     seq: 1
                   }}
                ]}
    end

    test "answers done for a snapshot at or below commit" do
      node =
        "m2"
        |> TestUtils.snapshotting_node!(TestUtils.terms_to(8, 1, 1, 1))
        |> TestUtils.committed!(8)

      meta = TestUtils.snapshot_meta(5, 1)
      chunk = TestUtils.install(meta, 0, 512)

      {_node, effects} = Node.step(node, {:message, chunk})

      assert [%InstallSnapshotReply{done?: true, offset: 2_048, seq: 1}] =
               TestUtils.sent(effects, InstallSnapshotReply)
    end

    test "waits for a pending persist before answering done" do
      node =
        "m2"
        |> TestUtils.snapshotting_node!(TestUtils.terms_to(8, 1, 1, 1))
        |> TestUtils.committed!(8)

      pending = %{node | persist_seq: 2}
      meta = TestUtils.snapshot_meta(5, 1)
      chunk = TestUtils.install(meta, 0, 512)

      {waiting, effects} = Node.step(pending, {:message, chunk})
      {_released, released} = Node.step(waiting, {:persisted, 2})

      assert TestUtils.done_flags(effects) == []
      assert TestUtils.done_flags(released) == [true]
    end

    test "stores a chunk and replies from the core" do
      node = TestUtils.snapshotting_node!("m2", [])
      meta = TestUtils.snapshot_meta(5, 1)
      chunk = TestUtils.install(meta, 0, 512, seq: 4)
      data = chunk.data

      {_node, effects} = Node.step(node, {:message, chunk})

      assert [_role, {:store_snapshot_chunk, ^meta, 0, ^data, false}, {:send, reply}] = effects

      assert reply == %InstallSnapshotReply{
               term: 1,
               from: "m2",
               to: "m1",
               offset: 512,
               done?: false,
               seq: 4
             }
    end

    test "answers the last chunk with the whole size and not done" do
      node = TestUtils.snapshotting_node!("m2", [])
      meta = TestUtils.snapshot_meta(5, 1, size: 512)
      chunk = TestUtils.install(meta, 0, 512)

      {_node, effects} = Node.step(node, {:message, chunk})

      assert [
               _role,
               {:store_snapshot_chunk, _meta, 0, _data, true},
               {:send, %{offset: 512, done?: false}}
             ] =
               effects
    end

    test "reports the offset it expects for a chunk out of order" do
      node = TestUtils.snapshotting_node!("m2", [])
      meta = TestUtils.snapshot_meta(5, 1)
      {receiving, _effects} = Node.step(node, {:message, TestUtils.install(meta, 0, 512)})
      late = TestUtils.install(meta, 1_024, 512)

      {_node, effects} = Node.step(receiving, {:message, late})

      assert [%InstallSnapshotReply{offset: 512, done?: false}] =
               TestUtils.sent(effects, InstallSnapshotReply)
    end

    test "restarts the receive at offset zero with another snapshot" do
      node = TestUtils.snapshotting_node!("m2", [])
      first = TestUtils.snapshot_meta(5, 1)
      newer = TestUtils.snapshot_meta(9, 1)
      {receiving, _effects} = Node.step(node, {:message, TestUtils.install(first, 0, 512)})

      {restarted, _effects} = Node.step(receiving, {:message, TestUtils.install(newer, 0, 256)})

      assert restarted.snapshot_receive.meta == newer
      assert restarted.snapshot_receive.received == 256
    end
  end

  describe "received" do
    test "ignores a snapshot nobody was receiving" do
      node = TestUtils.snapshotting_node!("m2", [])

      {same, effects} = Node.step(node, {:snapshot_received, TestUtils.snapshot_meta(5, 1)})

      assert {same, effects} == {node, []}
    end

    test "keeps a log suffix that matches the snapshot" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(14, 10, 1, 3))
      meta = TestUtils.snapshot_meta(10, 3)

      {installed, effects} = TestUtils.received!(node, meta)

      assert [{:compact_log, 11}, {:install_snapshot, ^meta}, {:persist, _seq, _state, []}] =
               effects

      assert {installed.log.first_index, installed.log.last_index} == {11, 14}
    end

    test "discards a log that conflicts with the snapshot" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(14, 10, 1, 2))
      meta = TestUtils.snapshot_meta(10, 3)

      {installed, effects} = TestUtils.received!(node, meta)

      assert [
               {:truncate_from, 11},
               {:compact_log, 11},
               {:install_snapshot, ^meta},
               {:persist, _, _, []}
             ] =
               effects

      assert {installed.log.first_index, installed.log.last_index, installed.log.last_term} ==
               {11, 10, 3}
    end

    test "discards a log that ends before the snapshot" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(6, 1, 1, 1))
      meta = TestUtils.snapshot_meta(10, 3)

      {installed, _effects} = TestUtils.received!(node, meta)

      assert {installed.log.first_index, installed.log.last_index} == {11, 10}
    end

    test "raises commit and applied to the snapshot index" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(6, 1, 1, 1))

      {installed, _effects} = TestUtils.received!(node, TestUtils.snapshot_meta(10, 3))

      assert {installed.commit, installed.applied, installed.apply_sent} == {10, 10, 10}
    end

    test "sends the done reply only after the install persist" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(6, 1, 1, 1))
      meta = TestUtils.snapshot_meta(10, 3)

      {installed, effects} = TestUtils.received!(node, meta)
      {_durable, released} = Node.step(installed, {:persisted, installed.persist_seq})

      assert TestUtils.done_flags(effects) == []
      assert TestUtils.done_flags(released) == [true]
    end

    test "holds the done reply behind an older persist" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(6, 1, 1, 1))
      pending = %{node | persist_seq: 2}

      {installed, _effects} = TestUtils.received!(pending, TestUtils.snapshot_meta(10, 3))
      {middle, older} = Node.step(installed, {:persisted, 2})
      {_last, newer} = Node.step(middle, {:persisted, installed.persist_seq})

      assert TestUtils.done_flags(older) == []
      assert TestUtils.done_flags(newer) == [true]
    end
  end

  describe "sending" do
    test "starts a send when the peer needs compacted entries" do
      leader = TestUtils.leader_with_snapshot!()

      {sending, effects} = Node.step(leader, {:message, TestUtils.failed_reply(leader, "m2", 0)})

      assert [{:send_snapshot, "m2", %{index: 5}, term, seq}] =
               Enum.filter(effects, &match?({:send_snapshot, _, _, _, _}, &1))

      assert {term, seq, sending.leader.progress["m2"].snapshot.offset} == {leader.term, 2, 0}
    end

    test "serves trailing entries as entries and older ones as a snapshot" do
      meta = TestUtils.snapshot_meta(8, 1)
      pairs = Enum.map(4..10, &{&1, 1})
      node = TestUtils.snapshotting_node!("m1", pairs, first_index: 4, snapshot: meta)

      {candidate, _effects} =
        node |> TestUtils.tick!(node.election_timeout - 1) |> Node.step(:tick)

      {durable, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})

      vote = %Keel.Raft.Message.RequestVoteReply{
        term: durable.term,
        from: "m2",
        to: "m1",
        granted?: true
      }

      {leader, _effects} = Node.step(durable, {:message, vote})

      {_entries, near} = Node.step(leader, {:message, TestUtils.failed_reply(leader, "m2", 5)})
      {_snapshot, far} = Node.step(leader, {:message, TestUtils.failed_reply(leader, "m3", 1)})

      assert {TestUtils.prev_indexes(near),
              Enum.any?(far, &match?({:send_snapshot, "m3", _, _, _}, &1))} == {[5], true}
    end

    test "ignores a reply of an abandoned send" do
      {sending, _meta, seq} = TestUtils.sending!()
      stale = TestUtils.snapshot_reply(sending, "m2", seq - 1, 512, false)

      {same, effects} = Node.step(sending, {:message, stale})

      assert {same, effects} == {sending, []}
    end

    test "acknowledges progress at a new offset" do
      {sending, _meta, seq} = TestUtils.sending!()
      reply = TestUtils.snapshot_reply(sending, "m2", seq, 512, false)

      {_moved, effects} = Node.step(sending, {:message, reply})

      assert effects == [{:snapshot_ack, "m2", 512}]
    end

    test "moves the peer to probe after the done reply" do
      {sending, meta, seq} = TestUtils.sending!()
      reply = TestUtils.snapshot_reply(sending, "m2", seq, meta.size, true)

      {done, effects} = Node.step(sending, {:message, reply})

      progress = done.leader.progress["m2"]

      assert {progress.snapshot, progress.match_index, progress.next_index} == {nil, 5, 6}

      assert [%AppendEntries{prev_index: 5, prev_term: 1}] =
               TestUtils.sent(effects, AppendEntries)
    end

    test "sends only empty appends while a snapshot is in flight" do
      {sending, _meta, _seq} = TestUtils.sending!()

      {_ticked, effects} = TestUtils.tick_effects!(sending, 2)

      assert [%AppendEntries{to: "m2", entries: []}] =
               effects |> TestUtils.sent(AppendEntries) |> Enum.filter(&(&1.to == "m2"))
    end

    test "replaces a send that heard nothing" do
      {sending, _meta, seq} = TestUtils.sending!()

      {_ticked, effects} = TestUtils.tick_effects!(sending, 4)

      assert [{:send_snapshot, "m2", _meta, _term, newer}] =
               Enum.filter(effects, &match?({:send_snapshot, _, _, _, _}, &1))

      assert newer > seq
    end

    test "waits for the install timeout once every byte arrived" do
      {sending, meta, seq} = TestUtils.sending!()
      reply = TestUtils.snapshot_reply(sending, "m2", seq, meta.size, false)
      {waiting, _effects} = Node.step(sending, {:message, reply})

      {late, early_effects} = TestUtils.tick_effects!(waiting, 8)
      {_later, later_effects} = TestUtils.tick_effects!(late, 4)

      assert {Enum.any?(early_effects, &match?({:send_snapshot, _, _, _, _}, &1)),
              Enum.any?(later_effects, &match?({:send_snapshot, _, _, _, _}, &1))} ==
               {false, true}
    end

    test "cancels the sends when it steps down" do
      {sending, _meta, _seq} = TestUtils.sending!()
      vote = TestUtils.request_vote("m3", "m1", sending.term + 1)

      {_follower, effects} = Node.step(sending, {:message, vote})

      assert {:cancel_snapshot_sends} in effects
    end
  end

  describe "taking" do
    test "asks for one snapshot at a time" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(10, 1, 1, 1))

      {asked, first} = Node.step(node, {:applied, 10})
      {_again, second} = Node.step(asked, {:applied, 10})

      assert {first, second} == {[{:take_snapshot, 10}], []}
    end

    test "keeps the trailing entries when the snapshot is durable" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(10, 1, 1, 1))
      {asked, _effects} = Node.step(node, {:applied, 10})
      meta = TestUtils.snapshot_meta(10, 1)

      {taken, effects} = Node.step(asked, {:snapshot_taken, meta})

      assert effects == [{:compact_log, 9}]
      assert {taken.log.first_index, Log.term_at(taken.log, 9)} == {9, 1}
    end

    test "ignores a snapshot older than the one held" do
      node = TestUtils.snapshotting_node!("m2", TestUtils.terms_to(10, 1, 1, 1))
      {asked, _effects} = Node.step(node, {:applied, 10})
      {taken, _effects} = Node.step(asked, {:snapshot_taken, TestUtils.snapshot_meta(10, 1)})

      {stale, effects} = Node.step(taken, {:snapshot_taken, TestUtils.snapshot_meta(4, 1)})

      assert {stale.log, effects} == {taken.log, []}
    end
  end
end
