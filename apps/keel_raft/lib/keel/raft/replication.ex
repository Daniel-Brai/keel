defmodule Keel.Raft.Replication do
  @moduledoc "The leader commits only an entry of its own term and counts only durable entries"

  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Durability
  alias Keel.Raft.Election
  alias Keel.Raft.Entry
  alias Keel.Raft.InvariantError
  alias Keel.Raft.Log
  alias Keel.Raft.Membership
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Node
  alias Keel.Raft.Reads
  alias Keel.Raft.Snapshot
  alias Keel.Raft.Transfer

  @doc "The entry takes the later of the given time and the time of the last entry."
  @spec propose(Node.t(), reference(), binary(), non_neg_integer()) ::
          {Node.t(), [Node.effect()]}
  def propose(%Node{role: :leader, leader: %{transfer: %{}}} = node, ref, _data, _time_ms),
    do: {node, [{:reply, ref, {:error, :transfer_in_progress}}]}

  def propose(%Node{role: :leader} = node, ref, data, time_ms) do
    entry = %Entry{
      term: node.term,
      index: node.log.last_index + 1,
      kind: :command,
      data: data,
      time_ms: max(time_ms, node.log.last_time_ms)
    }

    pending = Map.put(node.leader.pending, entry.index, ref)

    appended = %{
      node
      | log: Log.append(node.log, [entry]),
        leader: %{node.leader | pending: pending}
    }

    tracked = Snapshot.track(appended, [entry])
    {persisting, persist} = Durability.persist(tracked, false, [entry])
    {sent, appends} = replicate_all(persisting)

    {sent, [persist | appends]}
  end

  def propose(%Node{} = node, ref, _data, _time_ms),
    do: {node, [{:reply, ref, {:error, {:not_leader, node.leader_id}}}]}

  @spec replicate_all(Node.t()) :: {Node.t(), [Node.effect()]}
  def replicate_all(%Node{} = node) do
    node.leader.progress
    |> Map.keys()
    |> Enum.sort()
    |> Enum.reduce({node, []}, fn peer, {acc, effects} ->
      {sent, more} = replicate_to(acc, peer)
      {sent, effects ++ more}
    end)
  end

  @doc "A peer that is being sent a snapshot gets an empty append so its timer stays quiet."
  @spec heartbeat(Node.t()) :: {Node.t(), [Node.effect()]}
  def heartbeat(%Node{} = node) do
    {checked, restarts} = Snapshot.check_timeouts(node)

    {sent, effects} =
      checked.leader.progress
      |> Map.keys()
      |> Enum.sort()
      |> Enum.reduce({checked, []}, fn peer, {acc, effects} ->
        {sent, more} = probe(acc, peer)
        {sent, effects ++ more}
      end)

    {sent, restarts ++ effects}
  end

  @spec handle_append(Node.t(), AppendEntries.t()) :: {Node.t(), [Node.effect()]}
  def handle_append(%Node{role: :leader} = node, %AppendEntries{}), do: {node, []}

  def handle_append(%Node{} = node, %AppendEntries{} = append) do
    {follower, follow_effects} = Election.follow(node, append.from)
    trimmed = trim(follower.log, append)

    {handled, effects} =
      if consistent?(follower.log, trimmed),
        do: accept(follower, trimmed),
        else: reject(follower, trimmed)

    {handled, follow_effects ++ effects}
  end

  @spec refuse_append(Node.t(), AppendEntries.t()) :: {Node.t(), [Node.effect()]}
  def refuse_append(%Node{} = node, %AppendEntries{} = append) do
    reply = append_reply(node, append, false, 0)

    Durability.send_when_durable(node, reply)
  end

  @spec handle_reply(Node.t(), AppendEntriesReply.t()) :: {Node.t(), [Node.effect()]}
  def handle_reply(%Node{role: :leader} = node, %AppendEntriesReply{} = reply) do
    if Map.has_key?(node.leader.progress, reply.from),
      do: replied(node, reply),
      else: {node, []}
  end

  def handle_reply(%Node{} = node, %AppendEntriesReply{}), do: {node, []}

  @doc "Commits the highest index a quorum holds durably if its entry is from this term."
  @spec advance_commit(Node.t()) :: {Node.t(), [Node.effect()]}
  def advance_commit(%Node{role: :leader} = node) do
    candidate = quorum_match(node)

    if candidate > node.commit and commit_term_ok?(node, candidate) do
      {applied, effects} = apply_committed(%{node | commit: candidate})
      {started, read_effects} = Reads.maybe_start(applied)
      {settled, settle_effects} = Membership.committed(started)

      {settled, Enum.concat([effects, read_effects, settle_effects])}
    else
      {node, []}
    end
  end

  def advance_commit(%Node{} = node), do: {node, []}

  @doc "Sends nothing while the peer already has a full window of entries on the way."
  @spec replicate_to(Node.t(), T.member_id()) :: {Node.t(), [Node.effect()]}
  def replicate_to(%Node{} = node, peer), do: send_to(node, peer, :window)

  @spec put_progress(Node.t(), T.member_id(), Node.progress()) :: Node.t()
  def put_progress(node, peer, progress) do
    %{node | leader: %{node.leader | progress: Map.put(node.leader.progress, peer, progress)}}
  end

  defp replied(node, reply) do
    heard = Election.heard_from(node, reply.from)
    {confirmed, read_effects} = Reads.on_reply(heard, reply.from, reply.seq)
    progress = confirmed.leader.progress[reply.from]
    {replied, effects} = reply_to(confirmed, reply, progress)

    {replied, read_effects ++ effects}
  end

  # Only voters count. The leader counts itself only while it votes
  defp quorum_match(%Node{} = node) do
    matches =
      node.config
      |> Config.voters()
      |> Enum.map(&match_of(node, &1))
      |> Enum.sort(:desc)

    Enum.at(matches, Node.quorum(node) - 1, 0)
  end

  defp match_of(%Node{id: id} = node, id), do: node.durable_index

  defp match_of(%Node{} = node, peer) do
    case Map.fetch(node.leader.progress, peer) do
      {:ok, progress} -> progress.match_index
      :error -> 0
    end
  end

  defp reply_to(node, _reply, %{snapshot: %{}}), do: {node, []}

  defp reply_to(node, %AppendEntriesReply{success?: true} = reply, progress) do
    match = max(progress.match_index, reply.match_index)
    updated = %{progress | match_index: match, next_index: max(progress.next_index, match + 1)}

    acked = put_progress(node, reply.from, %{updated | pipelining?: true})
    {committed, effects} = advance_commit(acked)
    {nudged, nudges} = Transfer.on_reply(committed, reply.from)
    {caught_up, appends} = catch_up(nudged, reply.from)

    {caught_up, Enum.concat([effects, nudges, appends])}
  end

  # The reply's match index is the follower's last index so the leader skips straight past a gap
  defp reply_to(node, reply, progress) do
    next =
      progress.next_index
      |> Kernel.-(1)
      |> min(reply.match_index + 1)
      |> max(progress.match_index + 1)

    backed_off = %{progress | next_index: max(next, 1), pipelining?: false}
    backed = put_progress(node, reply.from, backed_off)

    replicate_to(backed, reply.from)
  end

  defp commit_term_ok?(%Node{opts: %{bug: :commit_old_term}}, _index), do: true
  defp commit_term_ok?(%Node{} = node, index), do: Log.term_at(node.log, index) == node.term

  @doc "Sends the peer a message with a new sequence number even when its window is full."
  @spec probe(Node.t(), T.member_id()) :: {Node.t(), [Node.effect()]}
  def probe(%Node{} = node, peer) do
    progress = node.leader.progress[peer]

    case progress.snapshot do
      nil -> send_to(node, peer, :probe)
      _sending -> send_heartbeat(node, peer, progress, 0)
    end
  end

  @doc "An empty append at the peer's match index. The peer holds that entry on disk so it answers without a sync."
  @spec confirm(Node.t(), T.member_id()) :: {Node.t(), [Node.effect()]}
  def confirm(%Node{} = node, peer) do
    progress = node.leader.progress[peer]
    match = progress.match_index

    cond do
      progress.snapshot != nil -> send_heartbeat(node, peer, progress, 0)
      match < node.log.first_index - 1 -> probe(node, peer)
      true -> send_heartbeat(node, peer, progress, match)
    end
  end

  # A full window gets an empty probe. A lost append then shows up as a refusal
  defp send_to(node, peer, mode) do
    progress = node.leader.progress[peer]

    cond do
      progress.snapshot != nil -> {node, []}
      progress.next_index < node.log.first_index -> Snapshot.start_send(node, peer)
      not window_full?(node, progress) -> send_entries(node, peer, progress, :batch)
      mode == :probe -> send_entries(node, peer, progress, :empty)
      true -> {node, []}
    end
  end

  defp window_full?(%Node{opts: opts}, %{pipelining?: true} = progress) do
    in_flight = progress.next_index - 1 - progress.match_index

    in_flight >= opts.max_append_entries * opts.max_inflight_appends
  end

  defp window_full?(_node, _progress), do: false

  # A follower that is still behind gets its next batch without waiting for a heartbeat
  defp catch_up(%Node{role: :leader} = node, peer) do
    case Map.fetch(node.leader.progress, peer) do
      {:ok, %{next_index: next}} when next <= node.log.last_index -> replicate_to(node, peer)
      _other -> {node, []}
    end
  end

  defp catch_up(%Node{} = node, _peer), do: {node, []}

  defp send_entries(node, peer, progress, size) do
    prev = progress.next_index - 1
    entries = entries_for(node, progress, size)
    seq = progress.seq + 1

    append = %AppendEntries{
      term: node.term,
      from: node.id,
      to: peer,
      prev_index: prev,
      prev_term: Log.term_at(node.log, prev),
      entries: entries,
      commit: node.commit,
      seq: seq
    }

    next = next_after(progress, entries)
    sent = put_progress(node, peer, %{progress | seq: seq, next_index: next})

    {sent, [{:send, append}]}
  end

  defp send_heartbeat(node, peer, progress, prev) do
    seq = progress.seq + 1

    append = %AppendEntries{
      term: node.term,
      from: node.id,
      to: peer,
      prev_index: prev,
      prev_term: heartbeat_term(node.log, prev),
      entries: [],
      commit: node.commit,
      seq: seq
    }

    sent = put_progress(node, peer, %{progress | seq: seq})

    {sent, [{:send, append}]}
  end

  defp heartbeat_term(_log, 0), do: 0
  defp heartbeat_term(log, prev), do: Log.term_at(log, prev)

  defp entries_for(_node, _progress, :empty), do: []

  defp entries_for(node, progress, :batch) do
    limit = node.opts.max_append_entries

    Log.slice(node.log, progress.next_index, node.log.last_index, limit)
  end

  defp next_after(%{pipelining?: true}, [_entry | _rest] = entries) do
    %Entry{index: last} = List.last(entries)

    last + 1
  end

  defp next_after(progress, _entries), do: progress.next_index

  # Entries the log already compacted are committed so they match
  defp trim(%Log{first_index: first} = log, %AppendEntries{prev_index: prev} = append)
       when prev < first - 1 do
    entries = Enum.drop(append.entries, first - 1 - prev)

    %{append | prev_index: first - 1, prev_term: log.base_term, entries: entries}
  end

  defp trim(_log, %AppendEntries{} = append), do: append

  defp consistent?(log, %AppendEntries{prev_index: prev, prev_term: prev_term}),
    do: Log.term_at(log, prev) == prev_term

  defp accept(node, %AppendEntries{} = append) do
    {merged, effects} = merge(node, append.entries)
    last_new = append.prev_index + length(append.entries)
    commit = max(merged.commit, min(append.commit, last_new))

    reply = append_reply(merged, append, true, last_new)
    {waiting, sends} = Durability.send_when_durable(%{merged | commit: commit}, reply)
    {applied, applies} = apply_committed(waiting)

    {applied, effects ++ sends ++ applies}
  end

  defp reject(node, append) do
    reply = append_reply(node, append, false, node.log.last_index)

    Durability.send_when_durable(node, reply)
  end

  # An entry already held with the same term is skipped. A different term truncates from there
  defp merge(node, entries) do
    case Enum.drop_while(entries, &(Log.term_at(node.log, &1.index) == &1.term)) do
      [] -> {node, []}
      [first | _rest] = new -> append_new(node, first, new)
    end
  end

  defp append_new(node, %Entry{index: index} = first, new) when index <= node.log.last_index do
    if index <= node.commit,
      do: raise(InvariantError, member: node.id, invariant: :truncate_committed, index: index)

    {truncated, truncate} = Durability.truncate(node, index)
    {appended, effects} = append_new(truncated, first, new)

    {appended, [truncate | effects]}
  end

  defp append_new(node, _first, new) do
    appended = Membership.refresh(%{node | log: Log.append(node.log, new)})
    tracked = Snapshot.track(appended, new)
    {persisting, persist} = Durability.persist(tracked, false, new)

    {persisting, [persist]}
  end

  defp apply_committed(%Node{commit: commit, apply_sent: sent} = node) when commit <= sent,
    do: {node, []}

  defp apply_committed(%Node{} = node) do
    entries = Log.slice(node.log, node.apply_sent + 1, node.commit, node.commit - node.apply_sent)
    {refs, waiting} = applied_refs(node, entries)

    {%{node | apply_sent: node.commit, leader: waiting}, [{:apply, entries, refs}]}
  end

  defp applied_refs(%Node{leader: nil}, _entries), do: {%{}, nil}

  defp applied_refs(%Node{leader: leader}, entries) do
    indexes = Enum.map(entries, & &1.index)
    {refs, pending} = Map.split(leader.pending, indexes)

    {refs, %{leader | pending: pending}}
  end

  @spec append_reply(Node.t(), AppendEntries.t(), boolean(), T.index()) ::
          AppendEntriesReply.t()
  defp append_reply(node, append, success?, match_index) do
    %AppendEntriesReply{
      term: node.term,
      from: node.id,
      to: append.from,
      success?: success?,
      match_index: match_index,
      seq: append.seq
    }
  end
end
