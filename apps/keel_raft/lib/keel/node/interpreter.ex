defmodule Keel.Node.Interpreter do
  alias Keel.Log.Writer
  alias Keel.Node.Applier
  alias Keel.Node.Book
  alias Keel.Node.SnapshotReceiver
  alias Keel.Node.SnapshotSender
  alias Keel.Node.State
  alias Keel.Node.Waiters
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.Node

  require Logger

  @spec run(State.t(), [Node.effect()]) :: State.t()
  def run(%State{} = state, effects), do: Enum.reduce(effects, state, &perform/2)

  @spec perform(Node.effect(), State.t()) :: State.t()
  def perform({:persist, seq, hard_state, entries}, %State{} = state) do
    :ok = Writer.persist(state.writer, seq, hard_state, entries)

    state
  end

  def perform({:truncate_from, index}, %State{} = state) do
    :ok = Writer.truncate_from(state.writer, index)

    state
  end

  def perform({:send, message}, %State{} = state) do
    :ok = state.transport.send(state.group, message.to, message)

    state
  end

  def perform({:apply, entries, refs}, %State{} = state) do
    {callers, waiters} = Waiters.take(state.waiters, refs)

    :ok = Applier.apply_entries(state.applier, entries, callers)

    forget_removed(%State{state | waiters: waiters}, entries)
  end

  def perform({:reply, ref, {:error, error}}, %State{} = state) do
    {from, waiters} = Waiters.pop(state.waiters, ref)
    refusal = refusal(state, error)

    if from, do: GenServer.reply(from, {:refused, refusal})

    %State{state | waiters: waiters, reads: Map.delete(state.reads, ref)}
  end

  def perform({:transferred, ref, target}, %State{} = state) do
    {from, waiters} = Waiters.pop(state.waiters, ref)

    if from, do: GenServer.reply(from, {:transferred, target})

    %State{state | waiters: waiters}
  end

  def perform({:read_ready, ref, index}, %State{} = state) do
    if state.applied >= index do
      answer_read(state, ref)
    else
      waits = Enum.sort([{index, ref} | state.read_waits])

      %State{state | read_waits: waits}
    end
  end

  def perform({:role_changed, role, term, leader}, %State{} = state) do
    Logger.info("member #{state.member_id} is #{role} in term #{term}")

    %State{state | role: role, leader: leader}
  end

  def perform({:take_snapshot, index}, %State{} = state) do
    :ok = Applier.take_snapshot(state.applier, index)

    state
  end

  def perform({:compact_log, keep_from}, %State{} = state) do
    :ok = Writer.compact_log(state.writer, keep_from)

    state
  end

  def perform({:install_snapshot, meta}, %State{} = state) do
    :ok = Applier.install_snapshot(state.applier, meta)

    state
  end

  def perform({:store_snapshot_chunk, meta, offset, data, done?}, %State{} = state) do
    :ok = SnapshotReceiver.chunk(state.receiver, meta, offset, data, done?)

    state
  end

  def perform({:send_snapshot, peer, meta, term, seq}, %State{} = state) do
    stopped = stop_sender(state, peer)

    case start_sender(stopped, peer, meta, term, seq) do
      {:ok, pid} -> %State{stopped | senders: Map.put(stopped.senders, peer, pid)}
      {:error, reason} -> sender_failed(stopped, peer, reason)
    end
  end

  def perform({:snapshot_ack, peer, offset}, %State{} = state) do
    case Map.fetch(state.senders, peer) do
      {:ok, pid} -> SnapshotSender.ack(pid, offset)
      :error -> :ok
    end

    state
  end

  def perform({:cancel_snapshot_sends}, %State{} = state) do
    Enum.each(state.senders, fn {_peer, pid} -> stop(state, pid) end)

    %State{state | senders: %{}}
  end

  @spec update_members(State.t(), Keel.Transport.members()) :: State.t()
  def update_members(%State{members: members} = state, members), do: state

  def update_members(%State{} = state, members) do
    :ok = state.transport.update_members(state.group, members)

    %State{state | members: members}
  end

  @doc "A read waits until the machine has applied the index its leader confirmed."
  @spec release_reads(State.t()) :: State.t()
  def release_reads(%State{} = state) do
    {ready, waiting} =
      Enum.split_while(state.read_waits, fn {index, _ref} -> index <= state.applied end)

    held = %State{state | read_waits: waiting}

    Enum.reduce(ready, held, fn {_index, ref}, acc -> answer_read(acc, ref) end)
  end

  # A removed member leaves the address book once the entry that removed it is applied
  defp forget_removed(%State{} = state, entries) do
    configs = for %Entry{kind: :config, data: data} <- entries, do: Config.decode!(data)

    Enum.reduce(configs, state, &forget(&2, &1))
  end

  defp forget(%State{} = state, %Config{} = config) do
    if MapSet.member?(config.removed, state.member_id),
      do: Logger.info("member #{state.member_id} was removed")

    members = Book.forget(state.members, config)

    update_members(state, members)
  end

  defp answer_read(%State{} = state, ref) do
    {query, reads} = Map.pop(state.reads, ref)
    {from, waiters} = Waiters.pop(state.waiters, ref)

    if from, do: Applier.query(state.applier, query, from)

    %State{state | reads: reads, waiters: waiters}
  end

  defp start_sender(state, peer, meta, term, seq) do
    opts = [
      disk: state.disk,
      snap_dir: state.snap_dir,
      meta: meta,
      term: term,
      seq: seq,
      from: state.member_id,
      to: peer,
      group: state.group,
      transport: state.transport,
      chunk_bytes: state.snapshot.chunk_bytes,
      max_inflight: state.snapshot.max_inflight,
      ack_timeout_ms: state.snapshot.ack_timeout_ms
    ]

    DynamicSupervisor.start_child(state.sender_supervisor, {SnapshotSender, opts})
  end

  defp stop_sender(%State{} = state, peer) do
    case Map.pop(state.senders, peer) do
      {nil, _senders} ->
        state

      {pid, senders} ->
        stop(state, pid)

        %State{state | senders: senders}
    end
  end

  defp stop(%State{} = state, pid),
    do: DynamicSupervisor.terminate_child(state.sender_supervisor, pid)

  # The core restarts the send when no acknowledgement comes
  defp sender_failed(%State{} = state, peer, reason) do
    Logger.warning("snapshot sender for #{peer} did not start: #{inspect(reason)}")

    state
  end

  defp refusal(state, {:not_leader, leader}), do: {:not_leader, State.address(state, leader)}
  defp refusal(_state, error), do: error
end
