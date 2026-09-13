defmodule Keel.Raft.Election do
  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Durability
  alias Keel.Raft.Entry
  alias Keel.Raft.Log
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Node
  alias Keel.Raft.Reads
  alias Keel.Raft.Replication
  alias Keel.Raft.Snapshot
  alias Keel.Raft.Transfer

  @spec tick(Node.t()) :: {Node.t(), [Node.effect()]}
  def tick(%Node{role: :leader} = node) do
    {transferring, transfer_effects} = Transfer.tick(node)
    {reading, read_effects} = Reads.tick(transferring)
    {beaten, effects} = check_and_beat(reading)

    {beaten, Enum.concat([transfer_effects, read_effects, effects])}
  end

  def tick(%Node{} = node) do
    {aging, expired} =
      Reads.expire_forwards(%{node | election_elapsed: node.election_elapsed + 1})

    if aging.election_elapsed >= aging.election_timeout do
      {started, effects} = timed_out(aging)

      {started, expired ++ effects}
    else
      {aging, expired}
    end
  end

  @doc "A leader counts as having heard from a leader."
  @spec leader_recent?(Node.t()) :: boolean()
  def leader_recent?(%Node{role: :leader}), do: true
  def leader_recent?(%Node{last_leader_tick: nil}), do: false

  def leader_recent?(%Node{} = node),
    do: node.ticks - node.last_leader_tick < node.opts.election_ticks_min

  @spec heard_from(Node.t(), T.member_id()) :: Node.t()
  def heard_from(%Node{leader: leader} = node, peer),
    do: %{node | leader: %{leader | active: MapSet.put(leader.active, peer)}}

  @spec start(Node.t()) :: {Node.t(), [Node.effect()]}
  def start(%Node{opts: %{pre_vote: true, bug: bug}} = node) when bug != :no_pre_vote,
    do: start_pre_vote(node)

  def start(%Node{} = node), do: campaign(node, false)

  # A member that does not vote waits for the next timer and never campaigns
  defp timed_out(%Node{} = node) do
    if Config.voter?(node.config, node.id),
      do: start(node),
      else: {reset_election_timer(node), []}
  end

  defp check_and_beat(%Node{} = node) do
    leader = %{node.leader | check_elapsed: node.leader.check_elapsed + 1}

    case check_quorum(%{node | leader: leader}) do
      {:alive, checked} -> beat(checked)
      :lost -> step_down(node, node.term, nil)
    end
  end

  defp check_quorum(%Node{opts: %{check_quorum: false}} = node), do: {:alive, node}

  defp check_quorum(%Node{leader: %{check_elapsed: elapsed}, opts: opts} = node)
       when elapsed < opts.election_ticks_min,
       do: {:alive, node}

  defp check_quorum(%Node{leader: leader} = node) do
    heard = Config.voting(node.config, leader.active) + Node.own_vote(node)
    window = %{leader | check_elapsed: 0, active: MapSet.new()}

    if keeps_leading?(node, heard), do: {:alive, %{node | leader: window}}, else: :lost
  end

  # A leader that removed itself keeps leading because only it can commit its removal
  defp keeps_leading?(%Node{} = node, heard),
    do: Node.own_vote(node) == 0 or heard >= Node.quorum(node)

  defp beat(%Node{} = node) do
    elapsed = node.heartbeat_elapsed + 1

    if elapsed >= node.opts.heartbeat_ticks do
      Replication.heartbeat(%{node | heartbeat_elapsed: 0})
    else
      {%{node | heartbeat_elapsed: elapsed}, []}
    end
  end

  @spec reset_election_timer(Node.t()) :: Node.t()
  def reset_election_timer(%Node{opts: opts, rng: rng} = node) do
    span = opts.election_ticks_max - opts.election_ticks_min + 1
    {draw, next} = :rand.uniform_s(span, rng)

    %{node | election_elapsed: 0, election_timeout: opts.election_ticks_min + draw - 1, rng: next}
  end

  @spec campaign(Node.t(), boolean()) :: {Node.t(), [Node.effect()]}
  def campaign(%Node{} = node, transfer?) do
    timed = reset_election_timer(node)
    raised = %{timed | role: :candidate, term: node.term + 1, voted_for: node.id, leader_id: nil}
    proposals = lost_proposals(node)
    forwards = lost_forwards(node, nil)
    lost = Enum.concat(proposals, forwards)

    {persisting, persist} = Durability.persist(raised, true, [])
    candidate = %{votes: MapSet.new(), campaign_seq: persisting.persist_seq, pre_vote?: false}
    campaigning = %{persisting | candidate: candidate, leader: nil, forwards: %{}}

    requests =
      for peer <- Config.voters(campaigning.config), peer != node.id do
        {:send, request_vote(campaigning, peer, campaigning.term, false, transfer?)}
      end

    {campaigning,
     lost ++ [{:role_changed, :candidate, campaigning.term, nil}, persist | requests]}
  end

  defp start_pre_vote(%Node{} = node) do
    timed = reset_election_timer(node)
    candidate = %{votes: MapSet.new([node.id]), campaign_seq: nil, pre_vote?: true}
    lost = lost_forwards(node, nil)

    asking = %{
      timed
      | role: :pre_candidate,
        leader_id: nil,
        candidate: candidate,
        forwards: %{}
    }

    requests =
      for peer <- Config.voters(asking.config), peer != node.id do
        {:send, request_vote(asking, peer, asking.term + 1, true, false)}
      end

    {tallied, won} = tally(asking)
    changed = {:role_changed, :pre_candidate, node.term, nil}

    cond do
      tallied.role != :pre_candidate -> {tallied, Enum.concat(lost, won)}
      node.role == :pre_candidate -> {tallied, Enum.concat(lost, requests)}
      true -> {tallied, Enum.concat(lost, [changed | requests])}
    end
  end

  @doc "The only path that lowers a role or adopts a higher term outside a campaign."
  @spec step_down(Node.t(), T.term_number(), nil | T.member_id()) ::
          {Node.t(), [Node.effect()]}
  def step_down(%Node{term: term} = node, new_term, leader_id) when new_term > term do
    voted = %{node | term: new_term, voted_for: nil}
    {follower, effects} = follow(voted, leader_id)
    {persisting, persist} = Durability.persist(follower, true, [])

    {persisting, [persist | effects]}
  end

  def step_down(%Node{} = node, _term, leader_id), do: follow(node, leader_id)

  @doc "Proposals still waiting on a leader get `:leadership_lost` because they may yet commit under the next leader."
  @spec follow(Node.t(), nil | T.member_id()) :: {Node.t(), [Node.effect()]}
  def follow(%Node{} = node, leader_id) do
    changed? = node.role != :follower or node.leader_id != leader_id

    lost =
      Enum.concat([
        lost_proposals(node),
        cancelled_sends(node),
        Reads.lost(node, leader_id),
        Transfer.lost(node, leader_id),
        lost_forwards(node, leader_id)
      ])

    timed = reset_election_timer(node)
    forwards = kept_forwards(node, leader_id)
    heard = heard_tick(node, leader_id)

    follower = %{
      timed
      | role: :follower,
        leader_id: leader_id,
        candidate: nil,
        leader: nil,
        forwards: forwards,
        last_leader_tick: heard
    }

    if changed? do
      {follower, Enum.concat(lost, [{:role_changed, :follower, follower.term, leader_id}])}
    else
      {follower, lost}
    end
  end

  @spec handle_pre_vote(Node.t(), RequestVote.t()) :: {Node.t(), [Node.effect()]}
  def handle_pre_vote(%Node{} = node, %RequestVote{} = request) do
    granted? = pre_vote_granted?(node, request)
    term = reply_term(node, request, granted?)
    reply = %{vote_reply(node, request, granted?) | term: term, pre_vote?: true}

    {node, [{:send, reply}]}
  end

  @spec handle_request_vote(Node.t(), RequestVote.t()) :: {Node.t(), [Node.effect()]}
  def handle_request_vote(%Node{} = node, %RequestVote{} = request) do
    if grant?(node, request) do
      timed = reset_election_timer(node)
      voted = %{timed | voted_for: request.from}

      {persisting, persist} = Durability.persist(voted, true, [])
      reply = vote_reply(persisting, request, true)
      {waiting, sends} = Durability.send_when_durable(persisting, reply)

      {waiting, [persist | sends]}
    else
      refuse_vote(node, request)
    end
  end

  @spec refuse_vote(Node.t(), RequestVote.t()) :: {Node.t(), [Node.effect()]}
  def refuse_vote(%Node{} = node, %RequestVote{} = request) do
    reply = vote_reply(node, request, false)

    Durability.send_when_durable(node, reply)
  end

  @spec handle_vote_reply(Node.t(), RequestVoteReply.t()) :: {Node.t(), [Node.effect()]}
  def handle_vote_reply(
        %Node{role: :pre_candidate, term: current} = node,
        %RequestVoteReply{pre_vote?: true, granted?: true, term: term} = reply
      )
      when term == current + 1 do
    votes = MapSet.put(node.candidate.votes, reply.from)

    tally(%{node | candidate: %{node.candidate | votes: votes}})
  end

  def handle_vote_reply(
        %Node{role: :candidate} = node,
        %RequestVoteReply{pre_vote?: false, granted?: true} = reply
      ) do
    votes = MapSet.put(node.candidate.votes, reply.from)

    tally(%{node | candidate: %{node.candidate | votes: votes}})
  end

  def handle_vote_reply(%Node{} = node, %RequestVoteReply{}), do: {node, []}

  @spec count_self_vote(Node.t()) :: {Node.t(), [Node.effect()]}
  def count_self_vote(%Node{role: :candidate, candidate: candidate} = node)
      when candidate.campaign_seq <= node.durable_seq do
    votes = MapSet.put(candidate.votes, node.id)

    tally(%{node | candidate: %{candidate | votes: votes}})
  end

  def count_self_vote(%Node{} = node), do: {node, []}

  defp lost_proposals(%Node{leader: %{pending: pending}}) do
    pending
    |> Enum.sort()
    |> Enum.map(fn {_index, ref} -> {:reply, ref, {:error, :leadership_lost}} end)
  end

  defp lost_proposals(%Node{}), do: []

  defp cancelled_sends(%Node{leader: %{progress: progress}}) do
    if Enum.any?(progress, fn {_peer, %{snapshot: snapshot}} -> snapshot != nil end),
      do: [{:cancel_snapshot_sends}],
      else: []
  end

  defp cancelled_sends(%Node{}), do: []

  defp lost_forwards(%Node{forwards: forwards, leader_id: current}, leader_id)
       when map_size(forwards) > 0 and current != leader_id do
    forwards
    |> Enum.sort()
    |> Enum.map(fn {_ref_id, {ref, _tick}} ->
      {:reply, ref, {:error, {:not_leader, leader_id}}}
    end)
  end

  defp lost_forwards(%Node{}, _leader_id), do: []

  defp kept_forwards(%Node{leader_id: leader_id, forwards: forwards}, leader_id), do: forwards
  defp kept_forwards(%Node{}, _leader_id), do: %{}

  defp heard_tick(%Node{last_leader_tick: tick}, nil), do: tick
  defp heard_tick(%Node{ticks: ticks}, _leader_id), do: ticks

  defp reply_term(_node, request, true), do: request.term
  defp reply_term(node, _request, false), do: node.term

  defp pre_vote_granted?(node, request) do
    voting?(node, request.from) and voting?(node, node.id) and request.term >= node.term and
      log_ok?(node, request) and not leader_recent?(node)
  end

  defp grant?(%Node{opts: %{bug: :double_vote}} = node, request),
    do: request.term == node.term and log_ok?(node, request)

  defp grant?(%Node{} = node, %RequestVote{} = request) do
    voting?(node, request.from) and voting?(node, node.id) and request.term == node.term and
      node.voted_for in [nil, request.from] and log_ok?(node, request)
  end

  defp voting?(%Node{config: config}, id), do: Config.voter?(config, id)

  # The candidate's log must be at least as up to date by last term and then last index
  defp log_ok?(%Node{log: log}, %RequestVote{} = request) do
    request.last_log_term > log.last_term or
      (request.last_log_term == log.last_term and request.last_log_index >= log.last_index)
  end

  defp tally(%Node{candidate: candidate} = node) do
    if Config.voting(node.config, candidate.votes) >= Node.quorum(node) do
      win(node)
    else
      {node, []}
    end
  end

  defp win(%Node{candidate: %{pre_vote?: true}} = node), do: campaign(node, false)
  defp win(%Node{} = node), do: become_leader(node)

  # A new leader appends a no-op of its own term so older entries commit with it
  defp become_leader(%Node{} = node) do
    next = node.log.last_index + 1

    noop = %Entry{
      term: node.term,
      index: next,
      kind: :noop,
      data: <<>>,
      time_ms: node.log.last_time_ms
    }

    progress =
      for peer <- Config.members(node.config), peer != node.id, into: %{} do
        {peer, %{next_index: next, match_index: 0, seq: 0, pipelining?: false, snapshot: nil}}
      end

    leader = %{
      node
      | role: :leader,
        leader_id: node.id,
        candidate: nil,
        leader: %{
          progress: progress,
          pending: %{},
          term_start_index: next,
          pending_config_index: Log.last_config_index(node.log),
          check_elapsed: 0,
          active: MapSet.new(),
          transfer: nil,
          reads: Reads.new()
        },
        heartbeat_elapsed: 0,
        log: Log.append(node.log, [noop])
    }

    tracked = Snapshot.track(leader, [noop])
    {persisting, persist} = Durability.persist(tracked, false, [noop])
    {sent, appends} = Replication.replicate_all(persisting)

    {sent, [{:role_changed, :leader, node.term, node.id}, persist | appends]}
  end

  defp request_vote(node, peer, term, pre_vote?, transfer?) do
    %RequestVote{
      term: term,
      from: node.id,
      to: peer,
      last_log_index: node.log.last_index,
      last_log_term: node.log.last_term,
      pre_vote?: pre_vote?,
      transfer?: transfer?
    }
  end

  defp vote_reply(node, request, granted?) do
    %RequestVoteReply{term: node.term, from: node.id, to: request.from, granted?: granted?}
  end
end
