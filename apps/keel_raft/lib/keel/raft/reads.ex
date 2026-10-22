defmodule Keel.Raft.Reads do
  @moduledoc "A leader answers a read only after a quorum replies to a heartbeat sent once the read began"

  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Message.ReadIndex
  alias Keel.Raft.Message.ReadIndexReply
  alias Keel.Raft.Node
  alias Keel.Raft.Replication

  @type request :: reference() | {:forwarded, T.member_id(), pos_integer()}

  @type round :: %{
          index: T.index(),
          seqs: %{T.member_id() => pos_integer()},
          acks: MapSet.t(T.member_id()),
          requests: [request()],
          started: non_neg_integer()
        }

  @type t :: %{waiting: [{non_neg_integer(), request()}], round: nil | round()}

  @spec new() :: t()
  def new, do: %{waiting: [], round: nil}

  @spec request(Node.t(), reference()) :: {Node.t(), [Node.effect()]}
  def request(%Node{role: :leader, leader: %{transfer: nil}} = node, ref),
    do: enqueue(node, ref)

  def request(%Node{role: :leader} = node, ref),
    do: {node, [{:reply, ref, {:error, :transfer_in_progress}}]}

  def request(%Node{role: :follower, leader_id: leader_id} = node, ref) when leader_id != nil,
    do: forward(node, ref)

  def request(%Node{} = node, ref),
    do: {node, [{:reply, ref, {:error, {:not_leader, node.leader_id}}}]}

  @spec handle_forward(Node.t(), ReadIndex.t()) :: {Node.t(), [Node.effect()]}
  def handle_forward(
        %Node{role: :leader, leader: %{transfer: nil}} = node,
        %ReadIndex{from: from} = message
      ) do
    if Config.member?(node.config, from),
      do: enqueue(node, {:forwarded, from, message.ref_id}),
      else: {node, []}
  end

  def handle_forward(%Node{} = node, %ReadIndex{} = message), do: refuse_forward(node, message)

  @spec refuse_forward(Node.t(), ReadIndex.t()) :: {Node.t(), [Node.effect()]}
  def refuse_forward(%Node{} = node, %ReadIndex{} = message),
    do: {node, [refusal(node, {:forwarded, message.from, message.ref_id})]}

  @spec handle_reply(Node.t(), ReadIndexReply.t()) :: {Node.t(), [Node.effect()]}
  def handle_reply(
        %Node{leader_id: leader, forwards: forwards} = node,
        %ReadIndexReply{from: leader, ref_id: ref_id} = reply
      ) do
    case Map.pop(forwards, ref_id) do
      {nil, _rest} -> {node, []}
      {{ref, _tick}, rest} -> {%{node | forwards: rest}, [answer(reply, ref)]}
    end
  end

  def handle_reply(%Node{} = node, %ReadIndexReply{}), do: {node, []}

  @spec on_reply(Node.t(), T.member_id(), pos_integer()) :: {Node.t(), [Node.effect()]}
  def on_reply(%Node{leader: %{reads: %{round: %{} = round}}} = node, peer, seq) do
    acks = acks_after(round, peer, seq)
    counted = put_round(node, %{round | acks: acks})

    if Config.voting(node.config, acks) + Node.own_vote(node) >= Node.quorum(node),
      do: confirm(counted, round),
      else: {counted, []}
  end

  def on_reply(%Node{} = node, _peer, _seq), do: {node, []}

  @spec maybe_start(Node.t()) :: {Node.t(), [Node.effect()]}
  def maybe_start(%Node{leader: %{reads: %{round: nil, waiting: [_first | _rest]}}} = node) do
    if node.commit >= node.leader.term_start_index, do: start_round(node), else: {node, []}
  end

  def maybe_start(%Node{} = node), do: {node, []}

  @spec tick(Node.t()) :: {Node.t(), [Node.effect()]}
  def tick(%Node{leader: %{reads: reads}} = node) do
    limit = node.opts.election_ticks_max
    {stale, fresh} = Enum.split_with(reads.waiting, fn {at, _} -> node.ticks - at >= limit end)
    failed = Enum.map(stale, &elem(&1, 1))

    {round, round_failed} = expire_round(node, reads.round)

    expired = put_reads(node, %{reads | waiting: fresh, round: round})
    effects = Enum.map(failed ++ round_failed, &refusal(node, &1))
    {started, started_effects} = maybe_start(expired)

    {started, effects ++ started_effects}
  end

  @spec expire_forwards(Node.t()) :: {Node.t(), [Node.effect()]}
  def expire_forwards(%Node{forwards: forwards} = node) when map_size(forwards) == 0,
    do: {node, []}

  def expire_forwards(%Node{forwards: forwards} = node) do
    limit = node.opts.election_ticks_min

    {stale, kept} =
      Enum.split_with(forwards, fn {_id, {_ref, at}} -> node.ticks - at >= limit end)

    effects = for {_id, {ref, _at}} <- Enum.sort(stale), do: {:reply, ref, {:error, :not_ready}}

    {%{node | forwards: Map.new(kept)}, effects}
  end

  @doc "A deposed leader refuses every read it still holds."
  @spec lost(Node.t(), nil | T.member_id()) :: [Node.effect()]
  def lost(%Node{leader: %{reads: reads}} = node, leader_id) do
    requests = Enum.map(reads.waiting, &elem(&1, 1)) ++ round_requests(reads.round)

    Enum.map(requests, &lost_effect(node, &1, leader_id))
  end

  def lost(%Node{}, _leader_id), do: []

  defp expire_round(_node, nil), do: {nil, []}

  defp expire_round(node, %{started: started} = round)
       when node.ticks - started >= node.opts.election_ticks_max,
       do: {nil, round.requests}

  defp expire_round(_node, round), do: {round, []}

  defp round_requests(nil), do: []
  defp round_requests(round), do: round.requests

  defp lost_effect(_node, ref, leader_id) when is_reference(ref),
    do: {:reply, ref, {:error, {:not_leader, leader_id}}}

  defp lost_effect(node, request, _leader_id), do: refusal(node, request)

  defp enqueue(%Node{} = node, request) do
    reads = node.leader.reads
    waiting = Enum.concat(reads.waiting, [{node.ticks, request}])

    node |> put_reads(%{reads | waiting: waiting}) |> maybe_start()
  end

  defp forward(%Node{} = node, ref) do
    ref_id = node.next_ref_id
    forwards = Map.put(node.forwards, ref_id, {ref, node.ticks})

    message = %ReadIndex{
      term: node.term,
      from: node.id,
      to: node.leader_id,
      ref_id: ref_id
    }

    {%{node | forwards: forwards, next_ref_id: ref_id + 1}, [{:send, message}]}
  end

  defp start_round(%Node{} = node) do
    requests = Enum.map(node.leader.reads.waiting, &elem(&1, 1))
    cleared = put_reads(node, %{node.leader.reads | waiting: []})

    if alone?(node) or node.opts.bug == :read_without_confirm,
      do: {cleared, ready(cleared, node.commit, requests)},
      else: probe(cleared, node.commit, requests)
  end

  defp alone?(%Node{config: config, id: id}), do: Config.voters(config) == [id]

  defp probe(node, index, requests) do
    peers = node.leader.progress |> Map.keys() |> Enum.sort()

    {sent, effects} =
      Enum.reduce(peers, {node, []}, fn peer, {acc, sends} ->
        {beaten, more} = Replication.confirm(acc, peer)

        {beaten, sends ++ more}
      end)

    seqs = Map.new(peers, fn peer -> {peer, sent.leader.progress[peer].seq} end)

    round = %{
      index: index,
      seqs: seqs,
      acks: MapSet.new(),
      requests: requests,
      started: node.ticks
    }

    {put_round(sent, round), effects}
  end

  defp confirm(%Node{} = node, round) do
    cleared = put_round(node, nil)
    {started, started_effects} = maybe_start(cleared)

    {started, ready(node, round.index, round.requests) ++ started_effects}
  end

  defp ready(node, index, requests), do: Enum.map(requests, &ready_effect(node, index, &1))

  defp ready_effect(_node, index, ref) when is_reference(ref), do: {:read_ready, ref, index}

  defp ready_effect(node, index, {:forwarded, from, ref_id}) do
    {:send,
     %ReadIndexReply{
       term: node.term,
       from: node.id,
       to: from,
       ref_id: ref_id,
       index: index,
       ok?: true
     }}
  end

  defp refusal(_node, ref) when is_reference(ref), do: {:reply, ref, {:error, :not_ready}}

  defp refusal(node, {:forwarded, from, ref_id}) do
    {:send,
     %ReadIndexReply{
       term: node.term,
       from: node.id,
       to: from,
       ref_id: ref_id,
       index: 0,
       ok?: false
     }}
  end

  defp answer(%ReadIndexReply{ok?: true, index: index}, ref), do: {:read_ready, ref, index}
  defp answer(%ReadIndexReply{}, ref), do: {:reply, ref, {:error, :not_ready}}

  defp acks_after(round, peer, seq) do
    case Map.fetch(round.seqs, peer) do
      {:ok, started} when started <= seq -> MapSet.put(round.acks, peer)
      _other -> round.acks
    end
  end

  defp put_reads(node, reads), do: %{node | leader: %{node.leader | reads: reads}}

  defp put_round(node, round), do: put_reads(node, %{node.leader.reads | round: round})
end
