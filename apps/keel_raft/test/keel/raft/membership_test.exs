defmodule Keel.Raft.MembershipTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.Config
  alias Keel.Raft.Log
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Node
  alias Keel.TestUtils

  describe "change/3" do
    test "fails on a follower with the leader it knows" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 1, "m1")

      {_follower, _ref, effects} = TestUtils.change!(follower, {:add_learner, "m4"})

      assert [{:reply, _ref, {:error, {:not_leader, "m1"}}}] = effects
    end

    test "fails before an entry of the term is committed" do
      leader = TestUtils.durable_leader!("m1", ~w[m1 m2 m3])

      {_leader, ref, effects} = TestUtils.change!(leader, {:add_learner, "m4"})

      assert effects == [{:reply, ref, {:error, :not_ready}}]
    end

    test "fails while another change is uncommitted" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {changed, _ref, _effects} = TestUtils.change!(leader, {:add_learner, "m4"})

      {_changed, ref, effects} = TestUtils.change!(changed, {:add_learner, "m5"})

      assert effects == [{:reply, ref, {:error, :change_in_progress}}]
    end

    test "fails while a transfer is running" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {transferring, _ref, _effects} = TestUtils.transferring!(leader, "m3")

      {_leader, ref, effects} = TestUtils.change!(transferring, {:add_learner, "m4"})

      assert effects == [{:reply, ref, {:error, :transfer_in_progress}}]
    end

    test "fails to add a voter as a learner" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {_leader, ref, effects} = TestUtils.change!(leader, {:add_learner, "m2"})

      assert effects == [{:reply, ref, {:error, :already_member}}]
    end

    test "fails to remove the last voter" do
      leader = TestUtils.single_voter_leader!("m1")

      {_leader, ref, effects} = TestUtils.change!(leader, {:remove, "m1"})

      assert effects == [{:reply, ref, {:error, :last_voter}}]
    end

    test "fails to promote a learner that never replied" do
      leader = TestUtils.learner_leader!()

      {_leader, ref, effects} = TestUtils.change!(leader, {:promote, "m4"})

      assert effects == [{:reply, ref, {:error, :not_caught_up}}]
    end

    test "fails to promote a learner that is far behind" do
      leader = TestUtils.learner_leader!()
      {heard, _effects} = TestUtils.ack(leader, "m4", 0)
      narrow = %{heard | opts: %{heard.opts | promote_lag_entries: 0}}

      {_leader, ref, effects} = TestUtils.change!(narrow, {:promote, "m4"})

      assert effects == [{:reply, ref, {:error, :not_caught_up}}]
    end

    test "fails to add an id that an earlier change removed" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      removed = TestUtils.committed_change!(leader, {:remove, "m3"}, "m2")

      {_leader, ref, effects} = TestUtils.change!(removed, {:add_learner, "m3"})

      assert effects == [{:reply, ref, {:error, :member_removed}}]
    end

    test "promotes a learner that caught up" do
      leader = TestUtils.learner_leader!()
      {heard, _effects} = TestUtils.ack(leader, "m4", leader.log.last_index)

      {promoted, _ref, _effects} = TestUtils.change!(heard, {:promote, "m4"})
      voters = Config.voters(promoted.config)

      assert voters == ~w[m1 m2 m3 m4]
    end

    test "counts the new voters before the entry commits" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {changed, _ref, _effects} = TestUtils.change!(leader, {:remove, "m3"})
      quorum = Node.quorum(changed)

      assert quorum == 2
    end

    test "sends the entry to a new learner" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {_changed, _ref, effects} = TestUtils.change!(leader, {:add_learner, "m4"})
      peers = TestUtils.sent_to(effects, AppendEntries)

      assert peers == ~w[m2 m3 m4]
    end

    test "answers the caller once the entry commits" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {changed, ref, _effects} = TestUtils.change!(leader, {:add_learner, "m4"})
      durable = TestUtils.persisted!(changed)

      {_committed, effects} = TestUtils.ack(durable, "m2", durable.log.last_index)

      assert [{:apply, [%{kind: :config}], %{} = refs}] = effects

      values = Map.values(refs)

      assert values == [ref]
    end
  end

  describe "a learner" do
    test "does not help commit an entry" do
      leader = TestUtils.learner_leader!()
      {proposed, _effects} = Node.step(leader, {:propose, make_ref(), "x", 1})
      durable = TestUtils.persisted!(proposed)

      {acked, _effects} = TestUtils.ack(durable, "m4", durable.log.last_index)

      assert acked.commit == leader.commit
    end

    test "never campaigns" do
      learner = TestUtils.learner_node!("m4")

      ticked = TestUtils.tick!(learner, 100)

      assert {ticked.role, ticked.term} == {:follower, 0}
    end

    test "ignores a transfer request" do
      learner = TestUtils.following!("m4", ~w[m1 m2 m3], 1, "m1")
      learned = TestUtils.append_config!(learner, TestUtils.random_config(learners: ["m4"]))

      {ignored, effects} = Node.step(learned, {:message, TestUtils.timeout_now("m1", "m4", 1)})

      assert {ignored.role, effects} == {:follower, []}
    end

    test "grants no vote" do
      learner = TestUtils.learner_node!("m4")
      request = TestUtils.request_vote("m1", "m4", 1)

      {refusing, _effects} = Node.step(learner, {:message, request})
      {_durable, effects} = Node.step(refusing, {:persisted, refusing.persist_seq})

      assert [{:send, %{granted?: false}}] = effects
    end
  end

  describe "a member with no config" do
    test "never campaigns" do
      hard_state = %Keel.Raft.HardState{term: 0, voted_for: nil, commit: 0}
      joiner = Node.new(%{id: "m4", voters: [], seed: {1, 2, 3}, hard_state: hard_state})

      ticked = TestUtils.tick!(joiner, 100)

      assert {ticked.role, ticked.term} == {:follower, 0}
    end

    test "takes its config from the first entry a leader sends" do
      joiner = Node.new(%{id: "m4", voters: [], seed: {1, 2, 3}})
      config = TestUtils.random_config(learners: ["m4"])
      entry = TestUtils.config_entry(1, 1, config)
      message = %{TestUtils.append(1, 0, 0, [entry], 0) | to: "m4"}

      {joined, _effects} = Node.step(joiner, {:message, message})

      assert joined.config == config
    end
  end

  describe "a follower" do
    test "uses a config entry once it is appended" do
      follower = TestUtils.follower_with_log!("m2", 1, TestUtils.entries([{1, 1}]))
      config = TestUtils.random_config(learners: ["m4"])

      appended = TestUtils.append_config!(follower, config)

      assert appended.config == config
    end

    test "goes back to the earlier config when the entry is truncated" do
      follower = TestUtils.follower_with_log!("m2", 1, TestUtils.entries([{1, 1}]))
      held = follower.config
      appended = TestUtils.append_config!(follower, TestUtils.random_config(learners: ["m4"]))
      replacement = TestUtils.entries([{2, 2}])
      message = TestUtils.append(2, 1, 1, replacement, 0)

      {truncated, _effects} = Node.step(appended, {:message, message})
      entry = Log.last_config_entry(truncated.log)

      assert truncated.config == held
      assert entry == nil
    end

    test "stops campaigning once its removal is appended" do
      follower = TestUtils.follower_with_log!("m2", 1, TestUtils.entries([{1, 1}]))
      config = TestUtils.random_config(voters: ~w[m1 m3], removed: ["m2"])
      removed = TestUtils.append_config!(follower, config)

      ticked = TestUtils.tick!(removed, 100)

      assert {ticked.role, ticked.term} == {:follower, 1}
    end
  end

  describe "a leader that removes itself" do
    test "keeps leading and sends entries until the entry commits" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      {removing, _ref, effects} = TestUtils.change!(leader, {:remove, "m1"})
      peers = TestUtils.sent_to(effects, AppendEntries)

      assert removing.role == :leader
      assert peers == ~w[m2 m3]
    end

    test "needs both other voters to commit" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])
      {removing, _ref, _effects} = TestUtils.change!(leader, {:remove, "m1"})
      durable = TestUtils.persisted!(removing)
      last = durable.log.last_index

      {one, _effects} = TestUtils.ack(durable, "m2", last)

      assert one.commit == leader.commit
    end

    test "steps down once the entry commits" do
      leader = TestUtils.committed_leader!("m1", ~w[m1 m2 m3])

      stepped = TestUtils.committed_change!(leader, {:remove, "m1"}, "m2")
      {both, effects} = TestUtils.ack(stepped, "m3", stepped.log.last_index)

      assert {both.role, both.leader} == {:follower, nil}
      assert {:role_changed, :follower, 1, nil} in effects
    end
  end

  describe "snapshots" do
    test "keep the config of the entry they cover" do
      config = TestUtils.random_config(learners: ["m4"])
      entries = [TestUtils.config_entry(1, 1, config) | TestUtils.entries([{2, 1}, {3, 1}])]
      follower = TestUtils.follower_with_log!("m2", 1, entries)
      meta = %{TestUtils.snapshot_meta(3, 1) | config: config}

      {compacted, _effects} = Node.step(follower, {:snapshot_taken, meta})

      assert {compacted.config, compacted.log.snapshot_config} == {config, config}
    end

    test "give a restored member the config they carry" do
      config = TestUtils.random_config(voters: ~w[m1 m2], removed: ["m3"])
      meta = %{TestUtils.snapshot_meta(3, 1) | config: config}

      restored =
        Node.new(%{
          id: "m1",
          voters: ~w[m1 m2 m3],
          seed: {1, 2, 3},
          snapshot: meta,
          first_index: 4
        })

      assert restored.config == config
    end
  end
end
