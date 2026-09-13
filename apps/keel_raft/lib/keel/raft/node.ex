defmodule Keel.Raft.Node do
  @moduledoc "Time arrives as `:tick` events and randomness comes from seeded state so a recorded run replays the same"

  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Durability
  alias Keel.Raft.Election
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.Log
  alias Keel.Raft.Membership
  alias Keel.Raft.Message
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Message.InstallSnapshot
  alias Keel.Raft.Message.InstallSnapshotReply
  alias Keel.Raft.Message.ReadIndex
  alias Keel.Raft.Message.ReadIndexReply
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Message.TimeoutNow
  alias Keel.Raft.Reads
  alias Keel.Raft.Replication
  alias Keel.Raft.Snapshot
  alias Keel.Raft.SnapshotMeta
  alias Keel.Raft.Transfer

  @enforce_keys ~w[id config opts rng election_timeout log]a
  defstruct [
    :id,
    :config,
    :opts,
    :rng,
    :election_timeout,
    :log,
    term: 0,
    voted_for: nil,
    role: :follower,
    leader_id: nil,
    election_elapsed: 0,
    heartbeat_elapsed: 0,
    candidate: nil,
    leader: nil,
    commit: 0,
    apply_sent: 0,
    durable_index: 0,
    persist_seq: 0,
    durable_seq: 0,
    hard_state_seq: 0,
    persist_marks: [],
    pending_sends: [],
    applied: 0,
    ticks: 0,
    log_bytes_since_snapshot: 0,
    snapshot_pending?: false,
    snapshot_receive: nil,
    last_leader_tick: nil,
    forwards: %{},
    next_ref_id: 1
  ]

  @type bug ::
          nil
          | :double_vote
          | :ack_before_durable
          | :commit_old_term
          | :keep_log_after_snapshot
          | :read_without_confirm
          | :no_pre_vote
          | :change_without_term_commit

  @type opts :: %{
          election_ticks_min: pos_integer(),
          election_ticks_max: pos_integer(),
          heartbeat_ticks: pos_integer(),
          max_append_entries: pos_integer(),
          max_inflight_appends: pos_integer(),
          snapshot_threshold_bytes: non_neg_integer(),
          snapshot_trailing_entries: non_neg_integer(),
          snapshot_ack_timeout_ticks: pos_integer(),
          snapshot_install_timeout_ticks: pos_integer(),
          pre_vote: boolean(),
          check_quorum: boolean(),
          promote_lag_entries: non_neg_integer(),
          bug: bug()
        }

  @type candidate :: %{
          votes: MapSet.t(T.member_id()),
          campaign_seq: nil | T.persist_seq(),
          pre_vote?: boolean()
        }

  @type sending :: %{
          meta: SnapshotMeta.t(),
          offset: non_neg_integer(),
          seq: pos_integer(),
          last_ack_tick: non_neg_integer()
        }

  @type progress :: %{
          next_index: T.index(),
          match_index: T.index(),
          seq: non_neg_integer(),
          pipelining?: boolean(),
          snapshot: nil | sending()
        }

  @type receiving :: %{
          meta: SnapshotMeta.t(),
          received: non_neg_integer(),
          seq: pos_integer(),
          from: T.member_id()
        }

  @type leader :: %{
          progress: %{T.member_id() => progress()},
          pending: %{T.index() => reference()},
          term_start_index: T.index(),
          pending_config_index: T.index(),
          check_elapsed: non_neg_integer(),
          active: MapSet.t(T.member_id()),
          transfer: nil | transfer(),
          reads: Reads.t()
        }

  @type transfer :: %{
          target: T.member_id(),
          ref: reference(),
          elapsed: non_neg_integer()
        }

  @type t :: %__MODULE__{
          id: T.member_id(),
          config: Config.t(),
          opts: opts(),
          rng: :rand.state(),
          election_timeout: pos_integer(),
          log: Log.t(),
          term: T.term_number(),
          voted_for: nil | T.member_id(),
          role: T.role(),
          leader_id: nil | T.member_id(),
          election_elapsed: non_neg_integer(),
          heartbeat_elapsed: non_neg_integer(),
          candidate: nil | candidate(),
          leader: nil | leader(),
          commit: T.index(),
          apply_sent: T.index(),
          durable_index: T.index(),
          persist_seq: T.persist_seq(),
          durable_seq: T.persist_seq(),
          hard_state_seq: T.persist_seq(),
          persist_marks: [{T.persist_seq(), T.index()}],
          pending_sends: [{T.persist_seq(), Message.t()}],
          applied: T.index(),
          ticks: non_neg_integer(),
          log_bytes_since_snapshot: non_neg_integer(),
          snapshot_pending?: boolean(),
          snapshot_receive: nil | receiving(),
          last_leader_tick: nil | non_neg_integer(),
          forwards: %{pos_integer() => {reference(), non_neg_integer()}},
          next_ref_id: pos_integer()
        }

  @type event ::
          :tick
          | {:message, Message.t()}
          | {:persisted, T.persist_seq()}
          | {:propose, reference(), binary(), non_neg_integer()}
          | {:applied, T.index()}
          | {:snapshot_taken, SnapshotMeta.t()}
          | {:snapshot_received, SnapshotMeta.t()}
          | :snapshot_failed
          | {:transfer_leadership, reference(), T.member_id()}
          | {:read_index, reference()}
          | {:change_membership, reference(), Config.change()}

  @type error ::
          {:not_leader, nil | T.member_id()}
          | :leadership_lost
          | :transfer_in_progress
          | :not_ready
          | :timeout
          | :invalid_target
          | :change_in_progress
          | :not_caught_up
          | Config.refusal()

  @type effect ::
          {:persist, T.persist_seq(), nil | HardState.t(), [Entry.t()]}
          | {:truncate_from, T.index()}
          | {:send, Message.t()}
          | {:apply, [Entry.t()], %{T.index() => reference()}}
          | {:reply, reference(), {:error, error()}}
          | {:role_changed, T.role(), T.term_number(), nil | T.member_id()}
          | {:take_snapshot, T.index()}
          | {:compact_log, T.index()}
          | {:send_snapshot, T.member_id(), SnapshotMeta.t(), T.term_number(), pos_integer()}
          | {:snapshot_ack, T.member_id(), non_neg_integer()}
          | {:cancel_snapshot_sends}
          | {:store_snapshot_chunk, SnapshotMeta.t(), non_neg_integer(), binary(), boolean()}
          | {:install_snapshot, SnapshotMeta.t()}
          | {:read_ready, reference(), T.index()}
          | {:transferred, reference(), T.member_id()}

  @type new_opts :: %{
          required(:id) => T.member_id(),
          required(:voters) => [T.member_id()],
          required(:seed) => {integer(), integer(), integer()},
          optional(:election_ticks_min) => pos_integer(),
          optional(:election_ticks_max) => pos_integer(),
          optional(:heartbeat_ticks) => pos_integer(),
          optional(:max_append_entries) => pos_integer(),
          optional(:max_inflight_appends) => pos_integer(),
          optional(:snapshot_threshold_bytes) => non_neg_integer(),
          optional(:snapshot_trailing_entries) => non_neg_integer(),
          optional(:snapshot_ack_timeout_ticks) => pos_integer(),
          optional(:snapshot_install_timeout_ticks) => pos_integer(),
          optional(:pre_vote) => boolean(),
          optional(:check_quorum) => boolean(),
          optional(:promote_lag_entries) => non_neg_integer(),
          optional(:hard_state) => HardState.t(),
          optional(:entries) => [Entry.t()],
          optional(:first_index) => T.index(),
          optional(:snapshot) => nil | SnapshotMeta.t(),
          optional(:bug) => bug()
        }

  @doc "A restored member starts as a follower with its durable term and vote and log."
  @spec new(new_opts()) :: t()
  def new(%{id: id, voters: voters, seed: seed} = new_opts) do
    hard_state = Map.get(new_opts, :hard_state, %HardState{term: 0, voted_for: nil, commit: 0})
    entries = Map.get(new_opts, :entries, [])
    first_index = Map.get(new_opts, :first_index, 1)
    snapshot = Map.get(new_opts, :snapshot)
    base = Config.new(voters)
    restored = Log.restore(entries, first_index, snapshot)
    log = Log.put_base_config(restored, base)
    pre_vote = Map.get(new_opts, :pre_vote, true)
    check_quorum = Map.get(new_opts, :check_quorum, true)

    if pre_vote != check_quorum,
      do: raise(ArgumentError, "pre_vote and check_quorum are on together or off together")

    opts = %{
      election_ticks_min: Map.get(new_opts, :election_ticks_min, 10),
      election_ticks_max: Map.get(new_opts, :election_ticks_max, 20),
      heartbeat_ticks: Map.get(new_opts, :heartbeat_ticks, 2),
      max_append_entries: Map.get(new_opts, :max_append_entries, 16),
      max_inflight_appends: Map.get(new_opts, :max_inflight_appends, 8),
      snapshot_threshold_bytes: Map.get(new_opts, :snapshot_threshold_bytes, 268_435_456),
      snapshot_trailing_entries: Map.get(new_opts, :snapshot_trailing_entries, 10_000),
      snapshot_ack_timeout_ticks: Map.get(new_opts, :snapshot_ack_timeout_ticks, 100),
      snapshot_install_timeout_ticks: Map.get(new_opts, :snapshot_install_timeout_ticks, 600),
      pre_vote: pre_vote,
      check_quorum: check_quorum,
      promote_lag_entries: Map.get(new_opts, :promote_lag_entries, 1_000),
      bug: new_opts[:bug]
    }

    node = %__MODULE__{
      id: id,
      config: Config.current(log),
      opts: opts,
      rng: :rand.seed_s(:exsss, seed),
      election_timeout: opts.election_ticks_max,
      log: log,
      term: hard_state.term,
      voted_for: hard_state.voted_for,
      durable_index: log.last_index,
      commit: log.snapshot_index,
      apply_sent: log.snapshot_index,
      applied: log.snapshot_index,
      log_bytes_since_snapshot: Log.bytes_after(log, log.snapshot_index)
    }

    Election.reset_election_timer(node)
  end

  @spec step(t(), event()) :: {t(), [effect()]}
  def step(%__MODULE__{} = node, :tick), do: Election.tick(%{node | ticks: node.ticks + 1})
  def step(%__MODULE__{} = node, {:persisted, seq}), do: Durability.persisted(node, seq)
  def step(%__MODULE__{} = node, {:applied, index}), do: Snapshot.applied(node, index)
  def step(%__MODULE__{} = node, {:snapshot_taken, meta}), do: Snapshot.taken(node, meta)
  def step(%__MODULE__{} = node, {:snapshot_received, meta}), do: Snapshot.received(node, meta)
  def step(%__MODULE__{} = node, :snapshot_failed), do: Snapshot.failed(node)

  def step(%__MODULE__{} = node, {:propose, ref, data, time_ms}),
    do: Replication.propose(node, ref, data, time_ms)

  def step(%__MODULE__{} = node, {:transfer_leadership, ref, target}),
    do: Transfer.start(node, ref, target)

  def step(%__MODULE__{} = node, {:read_index, ref}), do: Reads.request(node, ref)

  def step(%__MODULE__{} = node, {:change_membership, ref, change}),
    do: Membership.change(node, ref, change)

  # A pre-vote never moves a term so it skips the term rules
  def step(%__MODULE__{} = node, {:message, %RequestVote{pre_vote?: true} = request}),
    do: Election.handle_pre_vote(node, request)

  def step(
        %__MODULE__{} = node,
        {:message, %RequestVoteReply{pre_vote?: true, granted?: true} = reply}
      ),
      do: Election.handle_vote_reply(node, reply)

  def step(%__MODULE__{term: term} = node, {:message, %{term: message_term} = message})
      when message_term > term do
    if sticky?(node, message), do: {node, []}, else: adopt(node, message)
  end

  def step(%__MODULE__{term: term} = node, {:message, %{term: message_term} = message})
      when message_term < term,
      do: stale(node, message)

  def step(%__MODULE__{} = node, {:message, message}), do: handle(node, message)

  @spec quorum(t()) :: pos_integer()
  def quorum(%__MODULE__{config: config}), do: Config.quorum(config)

  @doc "A member counts its own acknowledgement only while it votes."
  @spec own_vote(t()) :: 0 | 1
  def own_vote(%__MODULE__{config: config, id: id}), do: vote_count(Config.voter?(config, id))

  @spec hard_state(t()) :: HardState.t()
  def hard_state(%__MODULE__{} = node),
    do: %HardState{term: node.term, voted_for: node.voted_for, commit: node.commit}

  @spec digest(t()) :: binary()
  def digest(%__MODULE__{} = node) do
    state =
      {node.id, node.term, node.voted_for, node.role, node.leader_id, node.election_elapsed,
       node.commit, node.log.last_index, node.log.last_term, node.log.first_index,
       node.log.snapshot_index, node.config}

    :crypto.hash(:sha256, :erlang.term_to_binary(state))
  end

  defp vote_count(true), do: 1
  defp vote_count(false), do: 0

  # A member that hears from a live leader ignores a higher term vote request
  defp sticky?(%__MODULE__{opts: %{check_quorum: true}} = node, %RequestVote{transfer?: false}),
    do: Election.leader_recent?(node)

  defp sticky?(%__MODULE__{}, _message), do: false

  defp adopt(node, message) do
    {released, finished} = Transfer.release(node, message.from)
    {stepped_down, down_effects} = Election.step_down(released, message.term, nil)
    {handled, effects} = handle(stepped_down, message)

    {handled, finished ++ down_effects ++ effects}
  end

  defp handle(node, %RequestVote{} = request), do: Election.handle_request_vote(node, request)
  defp handle(node, %RequestVoteReply{} = reply), do: Election.handle_vote_reply(node, reply)
  defp handle(node, %AppendEntries{} = append), do: Replication.handle_append(node, append)
  defp handle(node, %AppendEntriesReply{} = reply), do: Replication.handle_reply(node, reply)
  defp handle(node, %InstallSnapshot{} = install), do: Snapshot.handle_install(node, install)
  defp handle(node, %InstallSnapshotReply{} = reply), do: Snapshot.handle_reply(node, reply)
  defp handle(node, %TimeoutNow{} = message), do: Transfer.handle_timeout_now(node, message)
  defp handle(node, %ReadIndex{} = message), do: Reads.handle_forward(node, message)
  defp handle(node, %ReadIndexReply{} = reply), do: Reads.handle_reply(node, reply)

  # A stale request is refused with this member's term so the sender learns it
  defp stale(node, %RequestVote{} = request), do: Election.refuse_vote(node, request)
  defp stale(node, %AppendEntries{} = append), do: Replication.refuse_append(node, append)
  defp stale(node, %InstallSnapshot{} = install), do: Snapshot.refuse_install(node, install)
  defp stale(node, %ReadIndex{} = message), do: Reads.refuse_forward(node, message)
  defp stale(node, _reply), do: {node, []}
end
