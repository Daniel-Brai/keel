defmodule Keel.Raft.Durability do
  @moduledoc "A reply that claims durable state leaves only after that state is durable"

  alias Keel.Common.T
  alias Keel.Raft.Election
  alias Keel.Raft.Entry
  alias Keel.Raft.Log
  alias Keel.Raft.Membership
  alias Keel.Raft.Message
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Node
  alias Keel.Raft.Replication

  @spec persist(Node.t(), boolean(), [Entry.t()]) :: {Node.t(), Node.effect()}
  def persist(%Node{} = node, hard_state?, entries) do
    seq = node.persist_seq + 1
    hard_state = written_hard_state(node, hard_state?)
    marks = [{seq, node.log.last_index} | node.persist_marks]
    hard_state_seq = hard_state_seq(node, hard_state?, seq)

    persisting = %{node | persist_seq: seq, persist_marks: marks, hard_state_seq: hard_state_seq}

    {persisting, {:persist, seq, hard_state, entries}}
  end

  @doc "Dropped entries can never become durable. Nothing pending may still claim them."
  @spec truncate(Node.t(), T.index()) :: {Node.t(), Node.effect()}
  def truncate(%Node{} = node, index) do
    marks = Enum.map(node.persist_marks, fn {seq, last} -> {seq, min(last, index - 1)} end)
    durable = min(node.durable_index, index - 1)
    pending = Enum.reject(node.pending_sends, &acks_from?(&1, index))
    log = Log.truncate_from(node.log, index)

    cut = %{node | log: log, persist_marks: marks, durable_index: durable, pending_sends: pending}
    truncated = Membership.refresh(cut)

    {truncated, {:truncate_from, index}}
  end

  @spec send_when_durable(Node.t(), Message.t()) :: {Node.t(), [Node.effect()]}
  def send_when_durable(%Node{opts: %{bug: :ack_before_durable}} = node, message),
    do: {node, [{:send, message}]}

  def send_when_durable(%Node{durable_seq: durable, persist_seq: persist} = node, message)
      when durable >= persist,
      do: {node, [{:send, message}]}

  def send_when_durable(%Node{} = node, message) do
    case ready?(node, {node.persist_seq, message}) do
      true -> {node, [{:send, message}]}
      false -> {hold(node, message), []}
    end
  end

  # Persists complete in order and a finished one makes every entry up to its mark durable
  @spec persisted(Node.t(), T.persist_seq()) :: {Node.t(), [Node.effect()]}
  def persisted(%Node{} = node, seq) do
    durable_seq = max(node.durable_seq, seq)

    {done, marks} = Enum.split_with(node.persist_marks, fn {at, _last} -> at <= durable_seq end)

    durable_index =
      Enum.reduce(done, node.durable_index, fn {_at, last}, acc -> max(last, acc) end)

    synced = %{
      node
      | durable_seq: durable_seq,
        durable_index: durable_index,
        persist_marks: marks
    }

    {ready, waiting} = Enum.split_with(node.pending_sends, &ready?(synced, &1))
    released = %{synced | pending_sends: waiting}

    # Pending sends are newest first and leave in the order they were made
    sends = ready |> Enum.reverse() |> Enum.map(fn {_at, message} -> {:send, message} end)

    {counted, vote_effects} = Election.count_self_vote(released)
    {committed, commit_effects} = Replication.advance_commit(counted)

    {committed, sends ++ vote_effects ++ commit_effects}
  end

  defp acks_from?({_seq, %AppendEntriesReply{success?: true, match_index: match}}, index),
    do: match >= index

  defp acks_from?({_seq, _message}, _index), do: false

  defp written_hard_state(node, true), do: Node.hard_state(node)
  defp written_hard_state(_node, false), do: nil

  defp hard_state_seq(_node, true, seq), do: seq
  defp hard_state_seq(%Node{hard_state_seq: seq}, false, _seq), do: seq

  defp hold(node, message) do
    pending = [{node.persist_seq, message} | node.pending_sends]

    %{node | pending_sends: pending}
  end

  # An ack only claims its entries and its term. It need not wait for later writes
  defp ready?(%Node{} = node, {_at, %AppendEntriesReply{success?: true, match_index: match}}),
    do: node.durable_seq >= node.hard_state_seq and node.durable_index >= match

  defp ready?(%Node{} = node, {at, _message}), do: at <= node.durable_seq
end
