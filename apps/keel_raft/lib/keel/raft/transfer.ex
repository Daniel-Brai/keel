defmodule Keel.Raft.Transfer do
  @moduledoc "A pending transfer lives in the leader state so a step down always clears it"

  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Election
  alias Keel.Raft.Message.TimeoutNow
  alias Keel.Raft.Node
  alias Keel.Raft.Replication

  @spec start(Node.t(), reference(), T.member_id()) :: {Node.t(), [Node.effect()]}
  def start(%Node{role: :leader, leader: %{transfer: nil}} = node, ref, target) do
    if target != node.id and Config.voter?(node.config, target) do
      transfer = %{target: target, ref: ref, elapsed: 0}
      started = %{node | leader: %{node.leader | transfer: transfer}}

      nudge(started, target)
    else
      {node, [{:reply, ref, {:error, :invalid_target}}]}
    end
  end

  def start(%Node{role: :leader} = node, ref, _target),
    do: {node, [{:reply, ref, {:error, :transfer_in_progress}}]}

  def start(%Node{} = node, ref, _target),
    do: {node, [{:reply, ref, {:error, {:not_leader, node.leader_id}}}]}

  @doc "Each reply from the target may be the one that brings it up to date."
  @spec on_reply(Node.t(), T.member_id()) :: {Node.t(), [Node.effect()]}
  def on_reply(%Node{leader: %{transfer: %{target: target}}} = node, target),
    do: nudge(node, target)

  def on_reply(%Node{} = node, _peer), do: {node, []}

  @spec tick(Node.t()) :: {Node.t(), [Node.effect()]}
  def tick(%Node{leader: %{transfer: nil}} = node), do: {node, []}

  def tick(%Node{leader: %{transfer: transfer}} = node) do
    if transfer.elapsed + 1 >= node.opts.election_ticks_max do
      {%{node | leader: %{node.leader | transfer: nil}},
       [{:reply, transfer.ref, {:error, :timeout}}]}
    else
      moved = %{transfer | elapsed: transfer.elapsed + 1}

      {%{node | leader: %{node.leader | transfer: moved}}, []}
    end
  end

  @doc "A higher term message from the target means the transfer worked."
  @spec release(Node.t(), T.member_id()) :: {Node.t(), [Node.effect()]}
  def release(%Node{leader: %{transfer: %{target: from} = transfer}} = node, from),
    do: {%{node | leader: %{node.leader | transfer: nil}}, [{:transferred, transfer.ref, from}]}

  def release(%Node{} = node, _from), do: {node, []}

  @doc "A deposed leader answers a transfer it still holds."
  @spec lost(Node.t(), nil | T.member_id()) :: [Node.effect()]
  def lost(%Node{leader: %{transfer: %{ref: ref}}}, leader_id),
    do: [{:reply, ref, {:error, {:not_leader, leader_id}}}]

  def lost(%Node{}, _leader_id), do: []

  @spec handle_timeout_now(Node.t(), TimeoutNow.t()) :: {Node.t(), [Node.effect()]}
  def handle_timeout_now(
        %Node{role: :follower, leader_id: leader, term: term} = node,
        %TimeoutNow{from: leader, term: term}
      )
      when leader != nil do
    if Config.voter?(node.config, node.id),
      do: Election.campaign(node, true),
      else: {node, []}
  end

  def handle_timeout_now(%Node{} = node, %TimeoutNow{}), do: {node, []}

  defp nudge(%Node{leader: leader} = node, target) do
    progress = Map.fetch!(leader.progress, target)

    if progress.match_index == node.log.last_index do
      message = %TimeoutNow{term: node.term, from: node.id, to: target}

      {node, [{:send, message}]}
    else
      Replication.replicate_to(node, target)
    end
  end
end
