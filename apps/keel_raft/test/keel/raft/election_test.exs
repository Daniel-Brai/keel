defmodule Keel.Raft.ElectionTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.HardState
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Node
  alias Keel.TestUtils

  describe "campaign" do
    test "does not win before its own vote is durable" do
      node = TestUtils.timed_out_node!("m1", ["m1"])

      {candidate, _effects} = Node.step(node, :tick)

      assert candidate.role == :candidate
    end

    test "raises the term and votes for itself" do
      node = TestUtils.timed_out_node!("m1", ~w[m1 m2 m3])

      {candidate, effects} = Node.step(node, :tick)

      assert %Node{term: 1, voted_for: "m1"} = candidate
      assert {:persist, 1, %HardState{term: 1, voted_for: "m1", commit: 0}, []} in effects
    end

    test "asks every other voter" do
      node = TestUtils.timed_out_node!("m1", ~w[m1 m2 m3])

      {_candidate, effects} = Node.step(node, :tick)

      targets = TestUtils.sent_to(effects, RequestVote)

      assert targets == ~w[m2 m3]
    end

    test "wins alone once its vote is durable" do
      {candidate, _effects} = "m1" |> TestUtils.timed_out_node!(["m1"]) |> Node.step(:tick)

      {leader, effects} = Node.step(candidate, {:persisted, 1})

      assert leader.role == :leader
      assert {:role_changed, :leader, 1, "m1"} in effects
    end

    test "wins with a majority" do
      candidate = TestUtils.candidate!("m1", ~w[m1 m2 m3])
      reply = %RequestVoteReply{term: 1, from: "m2", to: "m1", granted?: true}

      {leader, _effects} = Node.step(candidate, {:message, reply})

      assert leader.role == :leader
    end
  end

  describe "vote" do
    test "refuses a lower term" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      request = TestUtils.request_vote("m1", "m2", 4)

      {_voter, effects} = Node.step(node, {:message, request})

      assert [{:send, %RequestVoteReply{granted?: false, term: 5}}] = effects
    end

    test "refuses a second candidate in one term" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      voted = TestUtils.voted!(node, "m1")
      request = TestUtils.request_vote("m3", "m2", 5)

      {_voter, effects} = Node.step(voted, {:message, request})

      assert [{:send, %RequestVoteReply{granted?: false}}] = effects
    end

    test "holds a granted reply until the vote is durable" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      request = TestUtils.request_vote("m1", "m2", 5)

      {_voter, effects} = Node.step(node, {:message, request})

      assert [{:persist, 2, %HardState{voted_for: "m1"}, []}] = effects
    end

    test "sends the granted reply when the vote is durable" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      request = TestUtils.request_vote("m1", "m2", 5)
      {voter, _effects} = Node.step(node, {:message, request})

      {_durable, effects} = Node.step(voter, {:persisted, 2})

      assert [{:send, %RequestVoteReply{granted?: true, to: "m1"}}] = effects
    end

    test "adopts a higher term before voting" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      request = TestUtils.request_vote("m1", "m2", 7)

      {voter, _effects} = Node.step(node, {:message, request})

      assert %Node{term: 7, voted_for: "m1"} = voter
    end
  end

  describe "step down" do
    test "leaves leadership on a higher term" do
      leader = TestUtils.leader!("m1", ~w[m1 m2 m3])
      reply = %RequestVoteReply{term: 9, from: "m2", to: "m1", granted?: false}

      {follower, _effects} = Node.step(leader, {:message, reply})

      assert %Node{role: :follower, term: 9, leader: nil, voted_for: nil} = follower
    end
  end

  describe "heartbeat" do
    test "ignores a stale heartbeat for the timer" do
      node = "m2" |> TestUtils.node_in_term!(~w[m1 m2 m3], 5) |> TestUtils.tick!(3)
      append = TestUtils.heartbeat("m1", "m2", 4)

      {follower, _effects} = Node.step(node, {:message, append})

      assert follower.election_elapsed == 3
    end

    test "follows the leader of its term" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      append = TestUtils.heartbeat("m1", "m2", 5)

      {follower, _effects} = Node.step(node, {:message, append})

      assert %Node{leader_id: "m1", election_elapsed: 0} = follower
    end

    test "is sent by a leader every heartbeat interval" do
      leader = "m1" |> TestUtils.leader!(~w[m1 m2 m3]) |> TestUtils.tick!(1)

      {_leader, effects} = Node.step(leader, :tick)

      targets = TestUtils.sent_to(effects, AppendEntries)

      assert targets == ~w[m2 m3]
    end
  end

  describe "new/1" do
    test "refuses pre-vote without check quorum" do
      opts = %{
        id: "m1",
        voters: ~w[m1 m2 m3],
        seed: {1, 2, 3},
        pre_vote: true,
        check_quorum: false
      }

      assert_raise ArgumentError, fn -> Node.new(opts) end
    end

    test "refuses check quorum without pre-vote" do
      opts = %{
        id: "m1",
        voters: ~w[m1 m2 m3],
        seed: {1, 2, 3},
        pre_vote: false,
        check_quorum: true
      }

      assert_raise ArgumentError, fn -> Node.new(opts) end
    end
  end

  describe "pre-vote campaign" do
    test "keeps the term and the vote" do
      node = TestUtils.pre_vote_node!("m1", ~w[m1 m2 m3])
      timed = TestUtils.tick!(node, node.election_timeout - 1)

      {pre, _effects} = Node.step(timed, :tick)

      assert %Node{role: :pre_candidate, term: 0, voted_for: nil} = pre
    end

    test "asks every other voter for the next term" do
      node = TestUtils.pre_vote_node!("m1", ~w[m1 m2 m3])
      timed = TestUtils.tick!(node, node.election_timeout - 1)

      {_pre, effects} = Node.step(timed, :tick)

      assert [%RequestVote{term: 1, pre_vote?: true}, %RequestVote{}] =
               TestUtils.sent(effects, RequestVote)
    end

    test "persists nothing before a quorum agrees" do
      node = TestUtils.pre_vote_node!("m1", ~w[m1 m2 m3])
      timed = TestUtils.tick!(node, node.election_timeout - 1)

      {_pre, effects} = Node.step(timed, :tick)

      assert Enum.filter(effects, &match?({:persist, _, _, _}, &1)) == []
    end

    test "campaigns once a quorum grants" do
      pre = TestUtils.pre_candidate!("m1", ~w[m1 m2 m3])
      grant = %RequestVoteReply{term: 1, from: "m2", to: "m1", granted?: true, pre_vote?: true}

      {candidate, _effects} = Node.step(pre, {:message, grant})

      assert %Node{role: :candidate, term: 1, voted_for: "m1"} = candidate
    end

    test "stays a pre-candidate when a voter refuses" do
      pre = TestUtils.pre_candidate!("m1", ~w[m1 m2 m3])
      refusal = %RequestVoteReply{term: 0, from: "m2", to: "m1", granted?: false, pre_vote?: true}

      {same, _effects} = Node.step(pre, {:message, refusal})

      assert same.role == :pre_candidate
    end

    test "takes the higher term of a refusing voter" do
      pre = TestUtils.pre_candidate!("m1", ~w[m1 m2 m3])
      refusal = %RequestVoteReply{term: 4, from: "m2", to: "m1", granted?: false, pre_vote?: true}

      {follower, _effects} = Node.step(pre, {:message, refusal})

      assert %Node{role: :follower, term: 4} = follower
    end

    test "ignores a real grant during the pre-vote round" do
      pre = TestUtils.pre_candidate!("m1", ~w[m1 m2 m3])
      grant = %RequestVoteReply{term: 0, from: "m2", to: "m1", granted?: true}

      {same, _effects} = Node.step(pre, {:message, grant})

      assert same.role == :pre_candidate
    end

    test "passes alone in a single voter cluster" do
      node = TestUtils.pre_vote_node!("m1", ["m1"])
      timed = TestUtils.tick!(node, node.election_timeout - 1)

      {_candidate, effects} = Node.step(timed, :tick)

      assert {:persist, 2, %HardState{term: 1, voted_for: "m1", commit: 0}, []} in effects
    end

    test "starts a new round when its timer runs out" do
      pre = TestUtils.pre_candidate!("m1", ~w[m1 m2 m3])
      timed = TestUtils.tick!(pre, pre.election_timeout - 1)

      {again, effects} = Node.step(timed, :tick)

      assert {again.role, length(TestUtils.sent(effects, RequestVote))} == {:pre_candidate, 2}
    end
  end

  describe "pre-vote request" do
    test "grants without changing anything" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      request = TestUtils.pre_vote_request("m1", "m2", 6)

      {same, effects} = Node.step(node, {:message, request})

      assert {same, effects} ==
               {node,
                [
                  {:send,
                   %RequestVoteReply{
                     term: 6,
                     from: "m2",
                     to: "m1",
                     granted?: true,
                     pre_vote?: true
                   }}
                ]}
    end

    test "refuses a lower term with its own term" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      request = TestUtils.pre_vote_request("m1", "m2", 4)

      {_same, effects} = Node.step(node, {:message, request})

      assert [{:send, %RequestVoteReply{term: 5, granted?: false, pre_vote?: true}}] = effects
    end

    test "refuses a log that is behind" do
      node = TestUtils.follower_with_log!("m2", 1, TestUtils.entries([{1, 1}, {2, 1}]))
      request = TestUtils.pre_vote_request("m1", "m2", 2, last_log_index: 1, last_log_term: 1)

      {_same, effects} = Node.step(node, {:message, request})

      assert [{:send, %RequestVoteReply{granted?: false}}] = effects
    end

    test "refuses while a leader was heard lately" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1", pre_vote: true)
      request = TestUtils.pre_vote_request("m3", "m2", 6)

      {_same, effects} = Node.step(follower, {:message, request})

      assert [{:send, %RequestVoteReply{granted?: false}}] = effects
    end

    test "grants once the leader has been quiet for the minimum timeout" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1", pre_vote: true)
      quiet = TestUtils.tick!(follower, follower.opts.election_ticks_min)
      request = TestUtils.pre_vote_request("m3", "m2", 6)

      {_same, effects} = Node.step(quiet, {:message, request})

      assert [{:send, %RequestVoteReply{granted?: true}}] = effects
    end

    test "is refused by a leader" do
      leader = TestUtils.pre_vote_leader!("m1", ~w[m1 m2 m3])
      request = TestUtils.pre_vote_request("m2", "m1", leader.term + 1)

      {same, effects} = Node.step(leader, {:message, request})

      assert {same.role, same.term, [{:send, %RequestVoteReply{granted?: false}}] = effects} ==
               {:leader, leader.term, effects}
    end

    test "is refused for a candidate that is not a voter" do
      node = TestUtils.node_in_term!("m2", ~w[m1 m2 m3], 5)
      request = TestUtils.pre_vote_request("m9", "m2", 6)

      {_same, effects} = Node.step(node, {:message, request})

      assert [{:send, %RequestVoteReply{granted?: false}}] = effects
    end
  end

  describe "stickiness" do
    test "ignores a higher term vote request while a leader was heard lately" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1", pre_vote: true)
      request = TestUtils.pre_vote_request("m3", "m2", 9, pre_vote?: false)

      {same, effects} = Node.step(follower, {:message, request})

      assert {same.term, effects} == {5, []}
    end

    test "answers a higher term vote request of a transfer" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1", pre_vote: true)
      request = TestUtils.pre_vote_request("m3", "m2", 6, pre_vote?: false, transfer?: true)

      {voter, _effects} = Node.step(follower, {:message, request})

      assert %Node{term: 6, voted_for: "m3"} = voter
    end

    test "is left off without check quorum" do
      follower = TestUtils.following!("m2", ~w[m1 m2 m3], 5, "m1")
      request = TestUtils.pre_vote_request("m3", "m2", 9, pre_vote?: false)

      {voter, _effects} = Node.step(follower, {:message, request})

      assert voter.term == 9
    end
  end

  describe "check quorum" do
    test "steps down when no quorum replied" do
      leader = TestUtils.pre_vote_leader!("m1", ~w[m1 m2 m3])

      {follower, _effects} = TestUtils.tick_effects!(leader, leader.opts.election_ticks_min)

      assert %Node{role: :follower, term: 1, leader: nil} = follower
    end

    test "persists nothing when it steps down" do
      leader = TestUtils.pre_vote_leader!("m1", ~w[m1 m2 m3])

      {_follower, effects} = TestUtils.tick_effects!(leader, leader.opts.election_ticks_min)

      assert Enum.filter(effects, &match?({:persist, _, _, _}, &1)) == []
    end

    test "keeps leading when a quorum replied" do
      leader = TestUtils.pre_vote_leader!("m1", ~w[m1 m2 m3])
      {heard, _effects} = TestUtils.reply_from!(leader, "m2", 1)

      {same, _effects} = TestUtils.tick_effects!(heard, heard.opts.election_ticks_min)

      assert same.role == :leader
    end

    test "counts only replies of the current window" do
      leader = TestUtils.pre_vote_leader!("m1", ~w[m1 m2 m3])
      {heard, _effects} = TestUtils.reply_from!(leader, "m2", 1)
      {quiet, _effects} = TestUtils.tick_effects!(heard, heard.opts.election_ticks_min)

      {follower, _effects} = TestUtils.tick_effects!(quiet, quiet.opts.election_ticks_min)

      assert follower.role == :follower
    end

    test "never steps down in a single voter cluster" do
      leader = TestUtils.pre_vote_node!("m1", ["m1"])
      timed = TestUtils.tick!(leader, leader.election_timeout - 1)
      {candidate, _effects} = Node.step(timed, :tick)
      {elected, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})

      {same, _effects} = TestUtils.tick_effects!(elected, elected.opts.election_ticks_min)

      assert same.role == :leader
    end
  end
end
