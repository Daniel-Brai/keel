defmodule Keel.Node.State do
  alias Keel.Common.T
  alias Keel.Env
  alias Keel.Log.Recovery.Result
  alias Keel.Node.Book
  alias Keel.Node.Waiters
  alias Keel.Raft.Node
  alias Keel.Transport

  @fields ~w[
    group
    member_id
    cluster_id
    members
    core
    env
    writer
    applier
    transport
    tick_ms
    max_pending
    max_mailbox
    waiters
    reads
    read_waits
    applied
    role
    leader
    dropped
    joining?
    receiver
    sender_supervisor
    senders
    disk
    snap_dir
    snapshot
  ]a
  @enforce_keys @fields
  defstruct @fields

  @timing ~w[
    election_ticks_min
    election_ticks_max
    heartbeat_ticks
    max_append_entries
    max_inflight_appends
    snapshot_threshold_bytes
    snapshot_trailing_entries
    snapshot_ack_timeout_ticks
    snapshot_install_timeout_ticks
    pre_vote
    check_quorum
    promote_lag_entries
  ]a

  @type t :: %__MODULE__{
          group: Transport.group(),
          member_id: T.member_id(),
          cluster_id: binary(),
          members: Transport.members(),
          core: Node.t(),
          env: Env.t(),
          writer: GenServer.server(),
          applier: GenServer.server(),
          transport: module(),
          tick_ms: pos_integer(),
          max_pending: pos_integer(),
          max_mailbox: pos_integer(),
          waiters: Waiters.t(),
          reads: %{reference() => term()},
          read_waits: [{T.index(), reference()}],
          applied: T.index(),
          role: T.role(),
          leader: nil | T.member_id(),
          dropped: non_neg_integer(),
          joining?: boolean(),
          receiver: GenServer.server(),
          sender_supervisor: Supervisor.supervisor(),
          senders: %{T.member_id() => pid()},
          disk: struct(),
          snap_dir: Path.t(),
          snapshot: snapshot()
        }

  @type snapshot :: %{
          chunk_bytes: pos_integer(),
          max_inflight: pos_integer(),
          ack_timeout_ms: pos_integer()
        }

  @type address :: nil | {T.member_id(), node()}

  @spec new(keyword(), Result.t()) :: t()
  def new(opts, %Result{} = recovered) do
    env = Keyword.get_lazy(opts, :env, &Env.os/0)
    member_id = Keyword.fetch!(opts, :member_id)
    raft = Keyword.get(opts, :raft, [])
    tick_ms = Keyword.get(raft, :tick_ms, 50)
    core = core(member_id, env, raft, recovered)
    members = opts |> Keyword.fetch!(:members) |> book(core, recovered)

    %__MODULE__{
      group: Keyword.fetch!(opts, :group),
      member_id: member_id,
      cluster_id: Keyword.fetch!(opts, :cluster_id),
      members: members,
      core: core,
      env: env,
      writer: Keyword.fetch!(opts, :writer),
      applier: Keyword.fetch!(opts, :applier),
      transport: Keyword.fetch!(opts, :transport),
      tick_ms: tick_ms,
      max_pending: Keyword.get(opts, :max_pending, 10_000),
      max_mailbox: Keyword.get(opts, :max_mailbox, 10_000),
      waiters: Waiters.new(),
      reads: %{},
      read_waits: [],
      applied: 0,
      role: :follower,
      leader: nil,
      dropped: 0,
      joining?: false,
      receiver: Keyword.fetch!(opts, :receiver),
      sender_supervisor: Keyword.fetch!(opts, :sender_supervisor),
      senders: %{},
      disk: Keyword.fetch!(opts, :disk),
      snap_dir: Keyword.fetch!(opts, :snap_dir),
      snapshot: snapshot(raft, tick_ms)
    }
  end

  @spec address(t(), nil | T.member_id()) :: address()
  def address(%__MODULE__{}, nil), do: nil

  def address(%__MODULE__{members: members}, member_id) do
    case Map.fetch(members, member_id) do
      {:ok, node} -> {member_id, node}
      :error -> nil
    end
  end

  defp snapshot(raft, tick_ms) do
    ticks = Keyword.get(raft, :snapshot_inflight_timeout_ticks, 20)

    %{
      chunk_bytes: Keyword.get(raft, :snapshot_chunk_bytes, 262_144),
      max_inflight: Keyword.get(raft, :snapshot_max_inflight_chunks, 4),
      ack_timeout_ms: ticks * tick_ms
    }
  end

  # The members come from the log and the snapshot
  defp core(member_id, env, raft, recovered) do
    options = %{
      id: member_id,
      voters: [],
      seed: env.seed,
      hard_state: recovered.hard_state,
      entries: recovered.entries,
      first_index: recovered.first_index,
      snapshot: recovered.snapshot
    }

    timing = raft |> Keyword.take(@timing) |> Map.new()

    merged = Map.merge(options, timing)

    Node.new(merged)
  end

  defp book(members, core, recovered) do
    followed = Book.follow(members, core.config)

    case recovered.snapshot do
      %{config: %{} = config} -> Book.forget(followed, config)
      _none -> followed
    end
  end
end
