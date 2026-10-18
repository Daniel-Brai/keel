defmodule Keel.Raft.Snapshot do
  @moduledoc "A log entry is dropped only below a snapshot that is durable on this member"

  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Durability
  alias Keel.Raft.Election
  alias Keel.Raft.Entry
  alias Keel.Raft.Log
  alias Keel.Raft.Message.InstallSnapshot
  alias Keel.Raft.Message.InstallSnapshotReply
  alias Keel.Raft.Node
  alias Keel.Raft.Reads
  alias Keel.Raft.Replication
  alias Keel.Raft.SnapshotMeta

  @spec track(Node.t(), [Entry.t()]) :: Node.t()
  def track(%Node{} = node, entries) do
    added = Enum.sum_by(entries, &Entry.bytes/1)

    %{node | log_bytes_since_snapshot: node.log_bytes_since_snapshot + added}
  end

  @spec applied(Node.t(), T.index()) :: {Node.t(), [Node.effect()]}
  def applied(%Node{} = node, index) do
    advanced = %{node | applied: max(node.applied, index)}

    maybe_take(advanced)
  end

  @spec failed(Node.t()) :: {Node.t(), [Node.effect()]}
  def failed(%Node{} = node), do: {%{node | snapshot_pending?: false}, []}

  @doc "The snapshot is durable. The log keeps the trailing entries and the store drops what lies below them."
  @spec taken(Node.t(), SnapshotMeta.t()) :: {Node.t(), [Node.effect()]}
  def taken(%Node{log: log} = node, %SnapshotMeta{index: index} = meta)
      when index > log.snapshot_index do
    trailing = node.opts.snapshot_trailing_entries
    keep_from = max(log.first_index, index - trailing + 1)
    compacted = Log.compact(log, meta, keep_from)
    bytes = Log.bytes_after(compacted, index)

    taken = %{node | log: compacted, log_bytes_since_snapshot: bytes, snapshot_pending?: false}

    {taken, [{:compact_log, keep_from}]}
  end

  def taken(%Node{} = node, %SnapshotMeta{}), do: failed(node)

  @spec start_send(Node.t(), T.member_id()) :: {Node.t(), [Node.effect()]}
  def start_send(%Node{} = node, peer) do
    progress = node.leader.progress[peer]
    meta = Log.snapshot_meta(node.log)
    seq = progress.seq + 1
    sending = %{meta: meta, offset: 0, seq: seq, last_ack_tick: node.ticks}
    updated = %{progress | seq: seq, snapshot: sending}
    started = Replication.put_progress(node, peer, updated)

    {started, [{:send_snapshot, peer, meta, node.term, seq}]}
  end

  @doc "A send that heard nothing for too long starts again with a new sequence number."
  @spec check_timeouts(Node.t()) :: {Node.t(), [Node.effect()]}
  def check_timeouts(%Node{leader: leader} = node) do
    stalled =
      for {peer, %{snapshot: %{} = snapshot}} <- leader.progress, stalled?(node, snapshot) do
        peer
      end

    stalled |> Enum.sort() |> Enum.reduce({node, []}, &restart/2)
  end

  @spec handle_reply(Node.t(), InstallSnapshotReply.t()) :: {Node.t(), [Node.effect()]}
  def handle_reply(%Node{role: :leader} = node, %InstallSnapshotReply{} = reply) do
    heard = Election.heard_from(node, reply.from)
    {confirmed, read_effects} = Reads.on_reply(heard, reply.from, reply.seq)
    progress = Map.get(confirmed.leader.progress, reply.from)

    case progress do
      %{snapshot: %{seq: seq} = snapshot} when seq == reply.seq ->
        {replied, effects} = current(confirmed, reply, progress, snapshot)

        {replied, read_effects ++ effects}

      _stale ->
        {confirmed, read_effects}
    end
  end

  def handle_reply(%Node{} = node, %InstallSnapshotReply{}), do: {node, []}

  @spec refuse_install(Node.t(), InstallSnapshot.t()) :: {Node.t(), [Node.effect()]}
  def refuse_install(%Node{} = node, %InstallSnapshot{} = message) do
    reply = reply(node, message.from, message.seq, 0, false)

    Durability.send_when_durable(node, reply)
  end

  @spec handle_install(Node.t(), InstallSnapshot.t()) :: {Node.t(), [Node.effect()]}
  def handle_install(%Node{role: :leader} = node, %InstallSnapshot{}), do: {node, []}

  def handle_install(%Node{} = node, %InstallSnapshot{meta: %{index: index}} = message)
      when index <= node.commit do
    {follower, follow_effects} = Election.follow(node, message.from)
    reply = reply(follower, message.from, message.seq, message.meta.size, true)
    {waiting, sends} = Durability.send_when_durable(follower, reply)

    {waiting, follow_effects ++ sends}
  end

  def handle_install(%Node{} = node, %InstallSnapshot{} = message) do
    {follower, follow_effects} = Election.follow(node, message.from)

    case receive_chunk(follower.snapshot_receive, message) do
      {:ok, receiving} -> accept_chunk(follower, receiving, message, follow_effects)
      :out_of_order -> out_of_order(follower, message, follow_effects)
    end
  end

  @doc "A matching log suffix is kept. Any other log is replaced."
  @spec received(Node.t(), SnapshotMeta.t()) :: {Node.t(), [Node.effect()]}
  def received(%Node{snapshot_receive: nil} = node, %SnapshotMeta{}), do: {node, []}

  def received(%Node{snapshot_receive: %{meta: held}} = node, %SnapshotMeta{} = meta)
      when held != meta,
      do: {node, []}

  def received(%Node{snapshot_receive: receiving} = node, %SnapshotMeta{index: index} = meta)
      when index <= node.commit do
    reply = reply(node, receiving.from, receiving.seq, meta.size, true)
    cleared = %{node | snapshot_receive: nil}

    Durability.send_when_durable(cleared, reply)
  end

  def received(%Node{snapshot_receive: receiving} = node, %SnapshotMeta{} = meta) do
    {log, log_effects} = reshape(node, meta)
    durable = max(min(node.durable_index, log.last_index), meta.index)
    marks = Enum.map(node.persist_marks, fn {seq, last} -> {seq, min(last, log.last_index)} end)

    installed = %{
      node
      | log: log,
        config: Config.current(log),
        snapshot_receive: nil,
        commit: max(node.commit, meta.index),
        applied: max(node.applied, meta.index),
        apply_sent: max(node.apply_sent, meta.index),
        durable_index: durable,
        persist_marks: marks,
        log_bytes_since_snapshot: Log.bytes_after(log, meta.index)
    }

    {persisting, persist} = Durability.persist(installed, true, [])
    reply = reply(persisting, receiving.from, receiving.seq, meta.size, true)
    {waiting, sends} = Durability.send_when_durable(persisting, reply)

    {waiting, log_effects ++ [{:install_snapshot, meta}, persist | sends]}
  end

  defp maybe_take(%Node{} = node) do
    due? = node.log_bytes_since_snapshot >= node.opts.snapshot_threshold_bytes
    fresh? = node.applied > node.log.snapshot_index

    if due? and fresh? and not node.snapshot_pending?,
      do: {%{node | snapshot_pending?: true}, [{:take_snapshot, node.applied}]},
      else: {node, []}
  end

  defp stalled?(%Node{} = node, snapshot) do
    limit =
      if snapshot.offset >= snapshot.meta.size,
        do: node.opts.snapshot_install_timeout_ticks,
        else: node.opts.snapshot_ack_timeout_ticks

    node.ticks - snapshot.last_ack_tick >= limit
  end

  defp restart(peer, {node, effects}) do
    {started, started_effects} = start_send(node, peer)

    {started, effects ++ started_effects}
  end

  defp current(node, %InstallSnapshotReply{done?: true} = reply, progress, snapshot) do
    match = max(progress.match_index, snapshot.meta.index)
    done = %{progress | match_index: match, next_index: match + 1, snapshot: nil}
    acked = Replication.put_progress(node, reply.from, done)
    {committed, commit_effects} = Replication.advance_commit(acked)
    {sent, send_effects} = Replication.replicate_to(committed, reply.from)

    {sent, commit_effects ++ send_effects}
  end

  defp current(node, %InstallSnapshotReply{offset: offset}, _progress, %{offset: offset}),
    do: {node, []}

  defp current(node, %InstallSnapshotReply{} = reply, progress, snapshot) do
    moved = %{snapshot | offset: reply.offset, last_ack_tick: node.ticks}
    updated = Replication.put_progress(node, reply.from, %{progress | snapshot: moved})

    {updated, [{:snapshot_ack, reply.from, reply.offset}]}
  end

  defp receive_chunk(_current, %InstallSnapshot{offset: 0} = message) do
    receiving = %{
      meta: message.meta,
      received: byte_size(message.data),
      seq: message.seq,
      from: message.from
    }

    {:ok, receiving}
  end

  defp receive_chunk(
         %{meta: meta, received: received} = current,
         %InstallSnapshot{meta: meta, offset: received} = message
       ) do
    {:ok, %{current | received: received + byte_size(message.data), seq: message.seq}}
  end

  defp receive_chunk(_current, %InstallSnapshot{}), do: :out_of_order

  defp accept_chunk(node, receiving, message, follow_effects) do
    store =
      {:store_snapshot_chunk, message.meta, message.offset, message.data, message.done?}

    reply = reply(node, message.from, message.seq, receiving.received, false)
    {waiting, sends} = Durability.send_when_durable(%{node | snapshot_receive: receiving}, reply)

    {waiting, Enum.concat(follow_effects, [store | sends])}
  end

  defp out_of_order(node, message, follow_effects) do
    reply =
      reply(node, message.from, message.seq, expected(node.snapshot_receive, message), false)

    {waiting, sends} = Durability.send_when_durable(node, reply)

    {waiting, follow_effects ++ sends}
  end

  defp expected(%{meta: meta, received: received}, %InstallSnapshot{meta: meta}), do: received
  defp expected(_current, %InstallSnapshot{}), do: 0

  defp reshape(%Node{log: log, opts: opts}, meta) do
    keep_from = meta.index + 1
    held = Log.term_at(log, meta.index)

    if held == meta.term or (opts.bug == :keep_log_after_snapshot and held != nil),
      do: {Log.compact(log, meta, keep_from), [{:compact_log, keep_from}]},
      else: {Log.reset(log, meta), [{:truncate_from, keep_from}, {:compact_log, keep_from}]}
  end

  defp reply(node, to, seq, offset, done?) do
    %InstallSnapshotReply{
      term: node.term,
      from: node.id,
      to: to,
      offset: offset,
      done?: done?,
      seq: seq
    }
  end
end
