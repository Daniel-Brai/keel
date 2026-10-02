defmodule KeelSim.Cluster do
  @moduledoc "Faults live in this interpreter and never in the core"

  alias Keel.Chain.Machine, as: ChainMachine
  alias Keel.Common.T
  alias Keel.Env.Disk
  alias Keel.KV.Machine, as: KvMachine
  alias Keel.Log.Recovery.Result
  alias Keel.Log.Snapshot
  alias Keel.Log.Store
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.InvariantError
  alias Keel.Raft.Message.ReadIndexReply
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Node
  alias Keel.Raft.SnapshotMeta
  alias Keel.Raft.SnapshotStream
  alias KeelSim.Client
  alias KeelSim.Coverage
  alias KeelSim.Disk, as: SimDisk
  alias KeelSim.FaultyKv
  alias KeelSim.History
  alias KeelSim.Invariants
  alias KeelSim.KvCheck
  alias KeelSim.Liveness
  alias KeelSim.Network
  alias KeelSim.Swarm
  alias KeelSim.Trace

  @action_ms 10
  @max_clients 4
  @heal_ops 2
  @max_joiners 3
  @kv_bugs [:kv_drop_put, :kv_no_dedupe]

  @enforce_keys ~w[ids members rand opts]a
  defstruct [
    :ids,
    :members,
    :rand,
    :opts,
    :heal,
    network: %Network{},
    invariants: %Invariants{},
    step: 0,
    trace_hash: <<0::256>>,
    schedule: [],
    coverage: %{},
    boots: %{},
    phase: :chaos,
    liveness: [],
    heal_used: 0,
    now: 0,
    clients: %{},
    history: [],
    refs: %{},
    replies: [],
    rounds: 0,
    founders: [],
    retired: MapSet.new(),
    trace: []
  ]

  @type syncing :: %{seq: nil | T.persist_seq(), hard_states: [HardState.t()]}

  @type work ::
          {:take, T.index()}
          | {:received, SnapshotMeta.t(), binary()}

  @type sender :: %{stream: SnapshotStream.t(), data: binary()}

  @type member :: %{
          node: nil | Node.t(),
          store: nil | Store.t(),
          disk: Disk.t(),
          syncing: nil | syncing(),
          machine: term(),
          applied: non_neg_integer(),
          applied_term: T.term_number(),
          applied_time_ms: non_neg_integer(),
          snapshots: [work()],
          receiving: nil | %{meta: SnapshotMeta.t(), data: binary()},
          senders: %{T.member_id() => sender()},
          config: nil | Config.t(),
          paused?: boolean(),
          reads: %{reference() => {Keel.KV.Command.op(), T.term_number()}},
          read_waits: [{T.index(), reference(), Keel.KV.Command.op()}]
        }

  @type opts :: %{
          required(:seed) => integer(),
          optional(:size) => pos_integer(),
          optional(:steps) => non_neg_integer(),
          optional(:heal_steps) => non_neg_integer(),
          optional(:faults) => [Swarm.fault()],
          optional(:faults?) => boolean(),
          optional(:weights) => map(),
          optional(:bug) => Swarm.bug(),
          optional(:crash_profile) => SimDisk.profile(),
          optional(:election_ticks_min) => pos_integer(),
          optional(:election_ticks_max) => pos_integer(),
          optional(:heartbeat_ticks) => pos_integer(),
          optional(:max_append_entries) => pos_integer(),
          optional(:fault_rate) => non_neg_integer(),
          optional(:segment_bytes) => pos_integer(),
          optional(:workload) => Swarm.workload(),
          optional(:clients) => pos_integer(),
          optional(:session_ttl_ms) => pos_integer(),
          optional(:max_sessions) => pos_integer(),
          optional(:check_max_states) => pos_integer(),
          optional(:skew_ms) => non_neg_integer(),
          optional(:pre_vote) => boolean(),
          optional(:bootstrap) => boolean(),
          optional(:trace) => boolean(),
          optional(:check_acks) => boolean(),
          optional(:stop_removed) => boolean(),
          optional(:promote_lag_entries) => non_neg_integer(),
          optional(:snapshot_threshold_bytes) => non_neg_integer(),
          optional(:snapshot_trailing_entries) => non_neg_integer(),
          optional(:snapshot_chunk_bytes) => pos_integer(),
          optional(:snapshot_max_inflight) => pos_integer(),
          optional(:snapshot_ack_timeout_ticks) => pos_integer(),
          optional(:snapshot_install_timeout_ticks) => pos_integer()
        }

  @type heal :: %{
          probe: nil | {T.index(), T.term_number()},
          committed?: boolean(),
          base: non_neg_integer()
        }

  @type t :: %__MODULE__{
          ids: [T.member_id()],
          members: %{T.member_id() => member()},
          rand: :rand.state(),
          opts: Swarm.config(),
          heal: nil | heal(),
          network: Network.t(),
          invariants: Invariants.t(),
          step: non_neg_integer(),
          trace_hash: binary(),
          schedule: [action()],
          coverage: Coverage.t(),
          boots: %{T.member_id() => pos_integer()},
          phase: :chaos | :heal,
          liveness: [{:liveness, Liveness.kind()}],
          heal_used: non_neg_integer(),
          now: non_neg_integer(),
          clients: %{pos_integer() => Client.t()},
          history: [History.event()],
          refs: %{reference() => {pos_integer(), non_neg_integer()} | :change},
          replies: [{reference(), Client.reply()}],
          rounds: non_neg_integer(),
          founders: [T.member_id()],
          retired: MapSet.t(T.member_id()),
          trace: [Trace.line()]
        }

  @type mode :: :sync | :delay | :fail_sync

  @type action ::
          {:tick, T.member_id()}
          | {:propose, T.member_id(), binary()}
          | {:client, pos_integer(), Client.choice()}
          | {:round, [T.member_id()]}
          | {:deliver, Network.link()}
          | {:drop, Network.link()}
          | {:duplicate, Network.link()}
          | {:complete_persist, T.member_id(), mode()}
          | {:crash, T.member_id()}
          | {:restart, T.member_id()}
          | {:snapshot_step, T.member_id()}
          | {:pause, T.member_id()}
          | {:resume, T.member_id()}
          | {:transfer, T.member_id(), T.member_id()}
          | {:join, T.member_id()}
          | {:change, T.member_id(), Config.change()}
          | {:isolate, T.member_id()}
          | :heal

  @type result :: %{
          steps: non_neg_integer(),
          heal_steps: non_neg_integer(),
          trace_hash: binary(),
          violations: [Invariants.violation()],
          liveness: [{:liveness, Liveness.kind()}],
          elections: non_neg_integer(),
          committed: non_neg_integer(),
          coverage: Coverage.t(),
          schedule: [action()],
          config: Swarm.config(),
          history: [History.event()],
          trace: [Trace.line()]
        }

  @spec new(opts()) :: t()
  def new(%{seed: seed} = opts) do
    config = Swarm.resolve(opts)
    ids = Enum.map(1..config.size, &"m#{&1}")
    rand = :rand.seed_s(:exsss, {seed, seed + 1, seed + 2})
    coverage = Coverage.new()
    clients = new_clients(config)

    cluster = %__MODULE__{
      ids: ids,
      founders: ids,
      members: %{},
      rand: rand,
      opts: config,
      coverage: coverage,
      clients: clients
    }

    ids
    |> Enum.reduce(cluster, &add_member(&2, &1))
    |> start_all()
  end

  @spec run(t()) :: result()
  def run(%__MODULE__{} = cluster) do
    cluster
    |> chaos()
    |> verify()
    |> result()
  end

  @doc "The heal phase follows unless `heal?` is false."
  @spec run_schedule(t(), [action()], boolean()) :: result()
  def run_schedule(%__MODULE__{} = cluster, schedule, heal? \\ true) do
    replayed = replay(cluster, schedule)
    finished = if heal?, do: verify(replayed), else: replayed

    result(finished)
  end

  @spec result(t()) :: result()
  def result(%__MODULE__{} = cluster) do
    checked = check_durable(cluster)
    events = closed_history(checked)
    found = history_violations(checked, events)
    invariants = Invariants.history_checked(checked.invariants, found)

    %{
      steps: checked.step,
      heal_steps: checked.heal_used,
      trace_hash: checked.trace_hash,
      violations: Enum.reverse(invariants.violations),
      liveness: checked.liveness,
      elections: invariants.elections,
      committed: map_size(invariants.committed),
      coverage: checked.coverage,
      schedule: Enum.reverse(checked.schedule),
      config: checked.opts,
      history: events,
      trace: Enum.reverse(checked.trace)
    }
  end

  @spec chaos(t()) :: t()
  def chaos(%__MODULE__{step: step, opts: %{steps: steps}} = cluster) when step >= steps,
    do: cluster

  def chaos(%__MODULE__{invariants: %Invariants{violations: [_first | _rest]}} = cluster),
    do: cluster

  def chaos(%__MODULE__{} = cluster) do
    cluster
    |> next_step()
    |> chaos()
  end

  @spec next_step(t()) :: t()
  def next_step(%__MODULE__{} = cluster) do
    {chosen, rand} = choose_action(cluster)
    {injected, action} = interpret(%{cluster | rand: rand}, chosen)

    apply_action(injected, action)
  end

  @spec act(t(), action()) :: t()
  def act(%__MODULE__{} = cluster, action) do
    message = "action is not enabled: #{inspect(action)}"

    if enabled?(cluster, action),
      do: apply_action(cluster, action),
      else: raise(ArgumentError, message)
  end

  @doc "Actions that are not enabled are skipped."
  @spec replay(t(), [action()]) :: t()
  def replay(%__MODULE__{} = cluster, schedule) do
    Enum.reduce_while(schedule, cluster, fn action, acc ->
      cond do
        acc.invariants.violations != [] -> {:halt, acc}
        enabled?(acc, action) -> {:cont, apply_action(acc, action)}
        true -> {:cont, acc}
      end
    end)
  end

  @doc "A miss adds a liveness entry that names the check that still fails."
  @spec verify(t()) :: t()
  def verify(%__MODULE__{} = cluster) do
    checked = check_durable(cluster)

    if checked.invariants.violations == [],
      do: checked |> begin_heal() |> heal_loop(),
      else: checked
  end

  @doc "Fault options play no part."
  @spec enabled?(t(), action()) :: boolean()
  def enabled?(%__MODULE__{} = cluster, {:client, slot, _choice}),
    do: Map.has_key?(cluster.clients, slot)

  def enabled?(%__MODULE__{} = cluster, {:round, _targets}), do: round_ready?(cluster)

  def enabled?(%__MODULE__{} = cluster, {:snapshot_step, id}),
    do: active?(cluster.members, id) and cluster.members[id].snapshots != []

  def enabled?(%__MODULE__{} = cluster, {:resume, id}),
    do: up?(cluster.members, id) and cluster.members[id].paused?

  def enabled?(%__MODULE__{} = cluster, {:transfer, id, _target}),
    do: active?(cluster.members, id)

  def enabled?(%__MODULE__{} = cluster, {:join, id}), do: not Map.has_key?(cluster.members, id)

  def enabled?(%__MODULE__{} = cluster, {:restart, id}) do
    members = cluster.members

    Map.has_key?(members, id) and not up?(members, id) and not retired?(cluster, id)
  end

  def enabled?(%__MODULE__{} = cluster, {:change, id, _change}),
    do: active?(cluster.members, id)

  def enabled?(%__MODULE__{} = cluster, action) do
    members = cluster.members

    case action do
      {:propose, id, _data} ->
        active?(members, id)

      {:complete_persist, id, _mode} ->
        active?(members, id) and members[id].syncing != nil

      {kind, id} when kind in [:tick, :pause] ->
        active?(members, id)

      {kind, id} when kind in [:crash, :isolate] ->
        up?(members, id)

      {kind, link} when kind in ~w[deliver drop duplicate]a ->
        link in ready_links(cluster)

      :heal ->
        Network.partitioned?(cluster.network)
    end
  end

  # A replayed schedule may name a member that its shortened form never created
  defp up?(members, id), do: match?(%{node: %Node{}}, Map.get(members, id))

  defp retired?(cluster, id), do: MapSet.member?(cluster.retired, id)

  defp active?(members, id), do: up?(members, id) and not members[id].paused?

  defp awake?(members, id), do: Map.has_key?(members, id) and not members[id].paused?

  defp round_ready?(cluster), do: length(ready_slots(cluster)) >= 2

  # A paused member takes no messages until it resumes
  defp ready_links(cluster) do
    cluster.network
    |> Network.ready_links()
    |> Enum.filter(fn {_from, to} -> awake?(cluster.members, to) end)
  end

  defp apply_action(cluster, action) do
    performed =
      try do
        perform(cluster, action)
      rescue
        error in InvariantError ->
          %{cluster | invariants: Invariants.raised(cluster.invariants, error.invariant)}
      end

    trace_hash = extend_hash(performed, action)

    schedule =
      if performed.phase == :chaos, do: [action | performed.schedule], else: performed.schedule

    %{
      performed
      | step: step_after(performed),
        trace_hash: trace_hash,
        schedule: schedule,
        now: performed.now + @action_ms
    }
  end

  defp step_after(%__MODULE__{phase: :chaos, step: step}), do: step + 1
  defp step_after(%__MODULE__{step: step}), do: step

  # Injected faults live here. A completion may be late or its sync may fail
  defp interpret(%__MODULE__{} = cluster, {:complete_persist, id, :sync} = action) do
    chance = cluster.opts.fault_rate

    if cluster.phase == :chaos and chance > 0,
      do: draw_persist_mode(cluster, id, chance),
      else: {cluster, action}
  end

  defp interpret(%__MODULE__{} = cluster, action), do: {cluster, action}

  defp draw_persist_mode(cluster, id, chance) do
    {draw, rand} = :rand.uniform_s(1_000, cluster.rand)
    failing = chance + max(div(chance, 10), 1)

    mode =
      cond do
        draw <= chance -> :delay
        draw <= failing -> :fail_sync
        true -> :sync
      end

    {%{cluster | rand: rand}, {:complete_persist, id, mode}}
  end

  defp add_member(cluster, id) do
    seed = seed_for(cluster, id, :disk)
    disk = SimDisk.new(profile: cluster.opts.crash_profile, seed: seed)

    member = %{
      node: nil,
      store: nil,
      disk: disk,
      syncing: nil,
      machine: init_machine(cluster),
      applied: 0,
      applied_term: 0,
      applied_time_ms: 0,
      config: nil,
      snapshots: [],
      receiving: nil,
      senders: %{},
      paused?: false,
      reads: %{},
      read_waits: []
    }

    %{cluster | members: Map.put(cluster.members, id, member)}
  end

  defp start_all(cluster), do: Enum.reduce(cluster.ids, cluster, &start_member(&2, &1))

  # Every seed comes from the run seed and the member. A replay never depends on draw order.
  defp seed_for(cluster, id, salt) do
    boots = Map.get(cluster.boots, id, 0)
    base = {cluster.opts.seed, id, salt, boots}
    first = :erlang.phash2({base, 1}, 1_000_000_007)
    second = :erlang.phash2({base, 2}, 1_000_000_007)
    third = :erlang.phash2({base, 3}, 1_000_000_007)

    {first, second, third}
  end

  defp start_member(cluster, id) do
    member = cluster.members[id]
    path = dir(id)
    store_opts = store_opts(cluster, id)

    case Store.open(member.disk, path, store_opts) do
      {:ok, store, result} -> bootstrap_member(cluster, id, store, result)
      {:error, disk, reason} -> fail_recovery(cluster, id, disk, reason)
    end
  end

  # A founder starts from one config entry. A joiner starts with nothing
  defp bootstrap_member(cluster, id, store, %Result{entries: [], snapshot: nil} = result) do
    if cluster.opts.bootstrap and id in cluster.founders do
      config = Config.new(cluster.founders)
      entry = %Entry{term: 0, index: 1, kind: :config, data: Config.encode(config), time_ms: 0}

      case Store.bootstrap(store, result, entry) do
        {:ok, synced, bootstrapped} -> boot_member(cluster, id, synced, bootstrapped)
        {:error, disk, reason} -> fail_recovery(cluster, id, disk, reason)
      end
    else
      boot_member(cluster, id, store, result)
    end
  end

  defp bootstrap_member(cluster, id, store, %Result{} = result),
    do: boot_member(cluster, id, store, result)

  defp boot_member(cluster, id, store, %Result{} = result) do
    booted = %{cluster | boots: Map.update(cluster.boots, id, 1, &(&1 + 1))}
    opts = booted.opts

    node_opts = %{
      id: id,
      voters: base_voters(booted, id),
      seed: seed_for(booted, id, :node),
      hard_state: result.hard_state,
      entries: result.entries,
      bug: node_bug(opts.bug),
      election_ticks_min: opts.election_ticks_min,
      election_ticks_max: opts.election_ticks_max,
      heartbeat_ticks: opts.heartbeat_ticks,
      max_append_entries: opts.max_append_entries,
      first_index: result.first_index,
      snapshot: result.snapshot,
      snapshot_threshold_bytes: opts.snapshot_threshold_bytes,
      snapshot_trailing_entries: opts.snapshot_trailing_entries,
      snapshot_ack_timeout_ticks: opts.snapshot_ack_timeout_ticks,
      snapshot_install_timeout_ticks: opts.snapshot_install_timeout_ticks,
      pre_vote: opts.pre_vote,
      check_quorum: opts.pre_vote,
      promote_lag_entries: opts.promote_lag_entries
    }

    member = booted.members[id]
    node = Node.new(node_opts)
    machine = init_machine(booted)

    started = %{
      member
      | node: node,
        store: store,
        syncing: nil,
        machine: machine,
        applied: 0,
        applied_term: 0,
        applied_time_ms: 0,
        config: nil,
        snapshots: [],
        receiving: nil,
        senders: %{},
        paused?: false,
        reads: %{},
        read_waits: []
    }

    invariants = Invariants.vote_recovered(booted.invariants, id, result.hard_state)
    noted = if result.torn_tail?, do: note(booted, :torn_tail_recovered), else: booted
    traced = trace_boot(noted, id, node)
    stored = %{put_member(traced, id, started) | invariants: invariants}

    restore_machine(stored, id, result.snapshot)
  end

  # Without a config entry or snapshot a founder falls back to the founders and a joiner to none
  defp base_voters(cluster, id) do
    case id in cluster.founders do
      true -> cluster.founders
      false -> []
    end
  end

  defp restore_machine(cluster, _id, nil), do: cluster

  defp restore_machine(cluster, id, %SnapshotMeta{} = meta) do
    case install_file(cluster, id, meta) do
      {:ok, restored} ->
        restored

      {:error, reason} ->
        %{cluster | invariants: Invariants.recovery_failed(cluster.invariants, id, reason)}
    end
  end

  # The machine takes the state of the snapshot file and every member's state at that index must agree
  defp install_file(cluster, id, meta) do
    member = cluster.members[id]
    module = machine_module(cluster)

    disk = Store.disk(member.store)

    with {:ok, disk, payload} <- Snapshot.read(disk, snap_dir(id), meta),
         {:ok, state} <- restore_state(cluster, payload) do
      store = %{member.store | disk: disk}

      installed = %{
        member
        | store: store,
          machine: state,
          applied: meta.index,
          applied_term: meta.term,
          applied_time_ms: meta.time_ms,
          config: meta.config
      }

      stored = put_member(cluster, id, installed)
      checked = check_snapshot(stored.invariants, stored, id, meta.index, module, state)

      {:ok, %{stored | invariants: checked}}
    else
      {:error, _disk, reason} -> {:error, {:snapshot_unreadable, reason}}
      {:error, reason} -> {:error, {:snapshot_unreadable, reason}}
    end
  end

  defp restore_state(%__MODULE__{opts: %{bug: bug}}, payload) when bug in @kv_bugs,
    do: FaultyKv.restore(bug, payload)

  defp restore_state(cluster, payload) do
    module = machine_module(cluster)

    module.restore(payload)
  end

  defp check_snapshot(invariants, cluster, id, index, module, state) do
    digest = :crypto.hash(:sha256, module.snapshot(state))
    checked = Invariants.snapshot_state(invariants, id, index, digest)

    if cluster.opts.workload == :chain, do: Invariants.chained(checked, id, state), else: checked
  end

  defp fail_recovery(cluster, id, disk, reason) do
    member = %{cluster.members[id] | disk: disk}
    invariants = Invariants.recovery_failed(cluster.invariants, id, reason)

    %{put_member(cluster, id, member) | invariants: invariants}
  end

  defp dir(id), do: "/sim/#{id}/log"

  defp snap_dir(id), do: "/sim/#{id}/snap"

  defp store_opts(cluster, id),
    do: [segment_bytes: cluster.opts.segment_bytes, member: id, snap_dir: snap_dir(id)]

  defp note(cluster, name), do: %{cluster | coverage: Coverage.hit(cluster.coverage, name)}

  defp restarted?(cluster, id), do: Map.get(cluster.boots, id, 0) > 1

  # Heal phase

  defp begin_heal(cluster) do
    seed = cluster.opts.seed
    healed = cluster |> resume_all() |> heal_network()
    down = Enum.reject(healed.ids, &(up?(healed.members, &1) or retired?(healed, &1)))
    restarted = Enum.reduce(down, healed, &(&2 |> start_member(&1) |> check_durable()))
    hash = extend_hash(restarted, :begin_heal)

    %{
      restarted
      | phase: :heal,
        rand: :rand.seed_s(:exsss, {seed, 41, 43}),
        heal: %{probe: nil, committed?: false, base: started(restarted)},
        trace_hash: hash
    }
  end

  defp heal_loop(%__MODULE__{invariants: %Invariants{violations: [_first | _rest]}} = cluster),
    do: cluster

  defp heal_loop(cluster) do
    settled = settle_probe(cluster)

    module = machine_module(settled)
    canonical = &module.snapshot/1

    case Liveness.status(settled.members, settled.ids, settled.heal.committed?, canonical) do
      :ok -> settle_clients(settled)
      {:pending, kind} -> heal_pending(settled, kind)
    end
  end

  # Clients that still wait get time to hear back before their commands end as unknown
  defp settle_clients(cluster) do
    waiting? = pending_clients(cluster) != [] or quota_unmet?(cluster)

    if waiting? and cluster.heal_used < cluster.opts.heal_steps,
      do: cluster |> heal_step() |> heal_loop(),
      else: cluster
  end

  defp heal_pending(cluster, kind) do
    if cluster.heal_used >= cluster.opts.heal_steps,
      do: %{cluster | liveness: [{:liveness, kind}]},
      else: cluster |> heal_step() |> heal_loop()
  end

  defp heal_step(cluster) do
    case heal_actions(cluster) do
      [] ->
        %{cluster | heal_used: cluster.opts.heal_steps}

      actions ->
        weights = heal_weights(actions, cluster)
        {action, rand} = pick_action(actions, weights, cluster.rand)

        {final, rand} = finalize(action, rand, cluster)

        performed = apply_action(%{cluster | rand: rand}, final)
        probed = track_probe(performed, final)

        %{probed | heal_used: probed.heal_used + 1}
    end
  end

  # Messages and syncs finish before time passes. A clean network is a fast one
  defp heal_weights(actions, cluster) do
    busy? = Enum.any?(actions, &(elem(&1, 0) not in [:tick, :client]))

    if busy?, do: %{cluster.opts.weights | tick: 1}, else: cluster.opts.weights
  end

  defp heal_actions(cluster) do
    up = Enum.filter(cluster.ids, &up?(cluster.members, &1))
    down = Enum.reject(cluster.ids, &(up?(cluster.members, &1) or retired?(cluster, &1)))
    ticks = Enum.map(up, &{:tick, &1})
    persists = for id <- up, cluster.members[id].syncing, do: {:complete_persist, id}
    snapshots = for id <- up, cluster.members[id].snapshots != [], do: {:snapshot_step, id}
    links = ready_links(cluster)
    deliveries = Enum.map(links, &{:deliver, &1})
    restarts = Enum.map(down, &{:restart, &1})
    probes = probe_actions(cluster)
    slots = heal_clients(cluster)
    waiting = Enum.map(slots, &{:client, &1})

    ticks ++ probes ++ deliveries ++ persists ++ snapshots ++ restarts ++ waiting
  end

  # One command goes to the leader after the heal. Its commit proves progress
  defp probe_actions(%__MODULE__{heal: %{committed?: true}}), do: []

  defp probe_actions(%__MODULE__{heal: %{probe: probe}} = cluster) do
    case Liveness.leader(cluster.members, cluster.ids) do
      {:ok, %Node{id: id, term: term}} when probe == nil or elem(probe, 1) != term ->
        [{:propose, id}]

      _other ->
        []
    end
  end

  defp track_probe(cluster, {:propose, id, _data}) do
    node = cluster.members[id].node

    if node.role == :leader,
      do: %{cluster | heal: %{cluster.heal | probe: {node.log.last_index, node.term}}},
      else: cluster
  end

  defp track_probe(cluster, _action), do: cluster

  defp settle_probe(%__MODULE__{heal: %{committed?: false, probe: probe}} = cluster)
       when probe != nil do
    {index, term} = probe

    case Map.get(cluster.invariants.committed, index) do
      {^term, _data} -> %{cluster | heal: %{cluster.heal | committed?: true}}
      _other -> cluster
    end
  end

  defp settle_probe(cluster), do: cluster

  defp resume_all(cluster) do
    Enum.reduce(cluster.ids, cluster, fn id, acc ->
      member = acc.members[id]

      put_member(acc, id, %{member | paused?: false})
    end)
  end

  defp heal_network(cluster) do
    isolated = Network.blocked_members(cluster.network)
    leader? = Enum.any?(isolated, &leader?(cluster.members[&1]))
    healed = %{cluster | network: Network.heal(cluster.network)}

    if leader?, do: note(healed, :heal_with_isolated_leader), else: healed
  end

  defp leader?(%{node: %Node{role: :leader}}), do: true
  defp leader?(_member), do: false

  # Choosing

  defp choose_action(cluster) do
    actions = chaos_actions(cluster)
    {action, rand} = pick_action(actions, cluster.opts.weights, cluster.rand)

    finalize(action, rand, cluster)
  end

  defp pick_action(actions, weights, rand) do
    total = actions |> Enum.map(&weight(&1, weights)) |> Enum.sum()
    {pick, next} = :rand.uniform_s(total, rand)

    {pick_weighted(actions, pick, weights), next}
  end

  defp pick_weighted([action | rest], pick, weights) do
    size = weight(action, weights)

    if pick <= size, do: action, else: pick_weighted(rest, pick - size, weights)
  end

  defp weight(:heal, weights), do: weights.heal
  defp weight({kind, _target}, weights), do: Map.fetch!(weights, kind)

  defp finalize({:propose, id}, rand, _cluster) do
    {data, next} = random_bytes(rand, 8)

    {{:propose, id, data}, next}
  end

  defp finalize({:client, slot}, rand, cluster) do
    {kind, after_kind} = draw_kind(rand)
    {key_ref, after_key} = draw_key(after_kind)
    {target, next} = draw_member(after_key, cluster.ids)

    {{:client, slot, {kind, key_ref, target}}, next}
  end

  defp finalize({:round, :all}, rand, cluster) do
    {targets, next} = Enum.map_reduce(1..@max_clients, rand, &draw_target(&1, &2, cluster.ids))

    {{:round, targets}, next}
  end

  defp finalize({:transfer, id}, rand, cluster) do
    others = List.delete(cluster.ids, id)
    {target, next} = draw_member(rand, others)

    {{:transfer, id, target}, next}
  end

  defp finalize({:membership, id}, rand, cluster) do
    {change, next} = draw_change(rand, cluster, id)

    {{:change, id, change}, next}
  end

  defp finalize({:complete_persist, id}, rand, _cluster),
    do: {{:complete_persist, id, :sync}, rand}

  defp finalize(action, rand, _cluster), do: {action, rand}

  # A fresh or removed id may be added. A learner may be promoted. Any member may be removed
  defp draw_change(rand, cluster, leader) do
    config = cluster.members[leader].node.config
    {roll, after_roll} = :rand.uniform_s(8, rand)

    cond do
      roll <= 3 -> draw_added(after_roll, cluster, config, leader)
      roll <= 6 -> draw_promoted(after_roll, cluster, config, leader)
      true -> draw_removed(after_roll, config, leader)
    end
  end

  defp draw_added(rand, cluster, config, leader) do
    candidates = Enum.reject(cluster.ids, &Config.member?(config, &1))

    case candidates do
      [] -> draw_removed(rand, config, leader)
      _some -> draw_one(rand, candidates, &{:add_learner, &1})
    end
  end

  # A cluster of one voter needs the new voter to win its next election. The new voter has not
  # seen the entry yet so no one promotes into it.
  defp draw_promoted(rand, cluster, config, leader) do
    case {Config.voters(config), Config.learners(config)} do
      {[_alone], _learners} -> draw_added(rand, cluster, config, leader)
      {_voters, []} -> draw_added(rand, cluster, config, leader)
      {_voters, learners} -> draw_one(rand, learners, &{:promote, &1})
    end
  end

  # A cluster of two voters needs both for every election. A member that holds a change
  # the leader lost can then block the other for good. So two voters never lose a voter.
  defp draw_removed(rand, config, leader) do
    case {Config.voters(config), Config.members(config)} do
      {[_first, _second], _members} -> draw_learner_removed(rand, config, leader)
      {[_alone], _members} -> draw_learner_removed(rand, config, leader)
      {_voters, members} -> draw_one(rand, members, &{:remove, &1})
    end
  end

  # Without a learner the change names the leader as a voter to promote. The core refuses it
  defp draw_learner_removed(rand, config, leader) do
    case Config.learners(config) do
      [] -> {{:promote, leader}, rand}
      learners -> draw_one(rand, learners, &{:remove, &1})
    end
  end

  defp draw_one(rand, candidates, change) do
    {id, next} = draw_member(rand, candidates)

    {change.(id), next}
  end

  # Writes and reads come in about the same number. Deletes are rare
  defp draw_kind(rand) do
    {roll, next} = :rand.uniform_s(12, rand)

    kind =
      cond do
        roll <= 5 -> :put
        roll <= 9 -> :get
        roll <= 11 -> :compare_and_set
        true -> :delete
      end

    {kind, next}
  end

  # Most keys are shared by every client. The rest belong to one client
  defp draw_key(rand) do
    {roll, next} = :rand.uniform_s(10, rand)
    key = if roll <= 6, do: {:shared, rem(roll, 3)}, else: {:own, rem(roll, 2)}

    {key, next}
  end

  defp draw_target(_slot, rand, ids), do: draw_member(rand, ids)

  defp draw_member(rand, ids) do
    count = length(ids)
    {at, next} = :rand.uniform_s(count, rand)

    {Enum.at(ids, at - 1), next}
  end

  defp chaos_actions(%__MODULE__{ids: ids, members: members} = cluster) do
    up = Enum.filter(ids, &up?(members, &1))
    down = Enum.reject(ids, &(up?(members, &1) or retired?(cluster, &1)))
    links = ready_links(cluster)
    awake = Enum.reject(up, &members[&1].paused?)
    ticks = Enum.map(awake, &{:tick, &1})
    proposals = proposals(cluster, awake)
    deliveries = Enum.map(links, &{:deliver, &1})
    persists = for id <- awake, members[id].syncing, do: {:complete_persist, id}
    snapshots = for id <- awake, members[id].snapshots != [], do: {:snapshot_step, id}
    restarts = Enum.map(down, &{:restart, &1})
    heal = if Network.partitioned?(cluster.network), do: [:heal], else: []
    base = ticks ++ proposals ++ deliveries ++ persists ++ snapshots ++ restarts ++ heal
    faults = fault_actions(cluster, awake, links)
    resumes = for id <- up, members[id].paused?, do: {:resume, id}

    base ++ faults ++ resumes
  end

  # The key value workload has clients in place of random proposals
  defp proposals(%__MODULE__{opts: %{workload: :chain}}, up), do: Enum.map(up, &{:propose, &1})

  defp proposals(%__MODULE__{} = cluster, _up) do
    slots = Map.keys(cluster.clients)
    clients = Enum.map(slots, &{:client, &1})
    rounds = if round_ready?(cluster), do: [{:round, :all}], else: []

    clients ++ rounds
  end

  defp fault_actions(cluster, up, links) do
    leaders = Enum.filter(up, &leader?(cluster.members[&1]))

    Enum.flat_map(cluster.opts.faults, fn
      :drop -> Enum.map(links, &{:drop, &1})
      :duplicate -> Enum.map(links, &{:duplicate, &1})
      :crash -> Enum.map(up, &{:crash, &1})
      :partition -> Enum.map(up, &{:isolate, &1})
      :pause -> Enum.map(up, &{:pause, &1})
      :transfer when length(cluster.ids) > 1 -> Enum.map(leaders, &{:transfer, &1})
      :transfer -> []
      :membership -> Enum.map(leaders, &{:membership, &1}) ++ joins(cluster)
    end)
  end

  # Every run may bring in a few fresh members. Each gets an id that was never used
  defp joins(cluster) do
    if length(cluster.ids) < length(cluster.founders) + @max_joiners,
      do: [{:join, "m#{length(cluster.ids) + 1}"}],
      else: []
  end

  # Performing

  defp perform(cluster, {:tick, id}), do: step_member(cluster, id, :tick)

  defp perform(cluster, {:propose, id, data}) do
    ref = make_ref()
    time_ms = wall_ms(cluster, id)

    step_member(cluster, id, {:propose, ref, data, time_ms})
  end

  defp perform(cluster, {:pause, id}) do
    member = cluster.members[id]

    put_member(cluster, id, %{member | paused?: true})
  end

  defp perform(cluster, {:resume, id}) do
    member = cluster.members[id]

    put_member(cluster, id, %{member | paused?: false})
  end

  defp perform(cluster, {:transfer, id, target}) do
    ref = make_ref()

    step_member(cluster, id, {:transfer_leadership, ref, target})
  end

  defp perform(cluster, {:join, id}) do
    ids = Enum.concat(cluster.ids, [id])

    %{cluster | ids: ids}
    |> add_member(id)
    |> start_member(id)
  end

  defp perform(cluster, {:change, id, change}) do
    ref = make_ref()
    before = cluster.members[id].node
    asked = %{cluster | refs: Map.put(cluster.refs, ref, :change)}
    stepped = step_member(asked, id, {:change_membership, ref, change})

    note_change(stepped, id, before, change)
  end

  defp perform(cluster, {:client, slot, choice}) do
    {client, outputs} = Client.step(cluster.clients[slot], cluster.now, choice)
    stepped = %{cluster | clients: Map.put(cluster.clients, slot, client)}

    run_outputs(stepped, slot, outputs)
  end

  # Every ready client starts a compare and set on one new key at the same step
  defp perform(cluster, {:round, targets}) do
    number = cluster.rounds + 1
    slots = ready_slots(cluster)
    round = %{cluster | rounds: number}

    Enum.reduce(slots, round, &join_round(&2, &1, targets, number))
  end

  defp perform(cluster, {:deliver, {_from, to} = link}) do
    {message, network} = Network.take(cluster.network, link)
    delivered = %{cluster | network: network}

    if delivered.members[to].node,
      do: delivered |> note_stale_vote(to, message) |> step_member(to, {:message, message}),
      else: delivered
  end

  defp perform(cluster, {:drop, link}) do
    {_message, network} = Network.take(cluster.network, link)
    dropped = %{cluster | network: network}

    note(dropped, :message_dropped)
  end

  defp perform(cluster, {:duplicate, link}) do
    network = Network.duplicate(cluster.network, link)
    duplicated = %{cluster | network: network}

    note(duplicated, :message_duplicated)
  end

  defp perform(cluster, {:complete_persist, id, mode}), do: complete_persist(cluster, id, mode)

  defp perform(cluster, {:snapshot_step, id}) do
    member = cluster.members[id]
    [work | rest] = member.snapshots
    queued = put_member(cluster, id, %{member | snapshots: rest})

    snapshot_work(queued, id, work)
  end

  defp perform(cluster, {:crash, id}) do
    store = cluster.members[id].store
    disk = Store.disk(store)

    crash_member(cluster, id, disk)
  end

  defp perform(cluster, {:restart, id}), do: cluster |> start_member(id) |> check_durable()

  defp perform(cluster, {:isolate, id}),
    do: %{cluster | network: Network.isolate(cluster.network, id, cluster.ids)}

  defp perform(cluster, :heal), do: heal_network(cluster)

  defp note_stale_vote(cluster, id, %RequestVote{term: term}) do
    node = cluster.members[id].node

    if node.term > term and restarted?(cluster, id),
      do: note(cluster, :stale_vote_after_restart),
      else: cluster
  end

  defp note_stale_vote(cluster, _id, _message), do: cluster

  # A sync covers every write made so far. A delayed one waits for a later step
  defp complete_persist(cluster, _id, :delay), do: note(cluster, :persist_delayed)

  defp complete_persist(cluster, id, mode) do
    member = cluster.members[id]
    store = if mode == :fail_sync, do: inject_sync_failure(member.store), else: member.store

    with {:ok, synced} <- Store.sync(store),
         {:ok, removed} <- Store.remove_compacted(synced) do
      synced(cluster, id, removed, member.syncing)
    else
      {:error, failed, _reason} ->
        noted = if mode == :fail_sync, do: note(cluster, :sync_failure_injected), else: cluster
        counted = note(noted, :sync_failure_crash)
        disk = Store.disk(failed)

        crash_member(counted, id, disk)
    end
  end

  defp inject_sync_failure(store),
    do: %{store | disk: SimDisk.inject(store.disk, :fail_next_sync)}

  defp synced(cluster, id, store, %{seq: seq, hard_states: hard_states}) do
    invariants =
      Enum.reduce(hard_states, cluster.invariants, &Invariants.vote_durable(&2, id, &1))

    member = %{cluster.members[id] | store: store, syncing: nil}
    durable = %{put_member(cluster, id, member) | invariants: invariants}

    if seq, do: step_member(durable, id, {:persisted, seq}), else: durable
  end

  # The crash drops what the profile drops
  defp crash_member(cluster, id, disk) do
    old = cluster.members[id]
    crashed = SimDisk.crash(disk)

    member = %{
      old
      | node: nil,
        store: nil,
        syncing: nil,
        disk: crashed,
        snapshots: [],
        receiving: nil,
        senders: %{},
        paused?: false,
        reads: %{},
        read_waits: []
    }

    cluster
    |> note_crash(old)
    |> put_member(id, member)
    |> check_durable()
  end

  defp note_crash(cluster, old) do
    pending = if old.syncing, do: note(cluster, :crash_with_pending_writes), else: cluster

    if leader?(old), do: note(pending, :leader_crashed), else: pending
  end

  defp step_member(cluster, id, event) do
    member = cluster.members[id]
    before = member.node

    {node, effects} = Node.step(before, event)
    stepped_terms = Invariants.term_stepped(cluster.invariants, id, before.term, node.term)
    invariants = check_acks(cluster, stepped_terms, id, node, effects)
    traced = trace_step(cluster, event, before, node, effects)

    stepped = put_member(%{traced | invariants: invariants}, id, %{member | node: node})
    applied = Enum.reduce(effects, stepped, &apply_effect(&2, id, before, &1))
    written = write_disk(applied, id, effects)
    noted = note_snapshot(written, id, before, effects)
    advanced = feed_applied(noted, id, effects)
    counted = note_batch(advanced, effects)
    drained = drain_reads(counted, id)
    delivered = deliver_replies(drained)

    retire_removed(delivered)
  end

  # Off only to show that the later loss check still sees an early ack
  defp check_acks(%__MODULE__{opts: %{check_acks: false}}, invariants, _id, _node, _effects),
    do: invariants

  defp check_acks(_cluster, invariants, id, node, effects),
    do: Invariants.acks_sent(invariants, id, node, effects)

  # An operator stops a member once the change that removed it is committed
  defp trace_step(%__MODULE__{opts: %{trace: true}} = cluster, event, before, node, effects) do
    case Trace.step(event, before, node, effects, cluster.step) do
      {:ok, line} -> %{cluster | trace: [line | cluster.trace]}
      :skip -> cluster
    end
  end

  defp trace_step(%__MODULE__{} = cluster, _event, _before, _node, _effects), do: cluster

  defp trace_restart(%__MODULE__{opts: %{trace: true}} = cluster, node) do
    line = Trace.restart(node, cluster.step)

    %{cluster | trace: [line | cluster.trace]}
  end

  defp trace_restart(%__MODULE__{} = cluster, _node), do: cluster

  defp trace_boot(cluster, id, node) do
    case Map.fetch!(cluster.boots, id) do
      1 -> cluster
      _restart -> trace_restart(cluster, node)
    end
  end

  defp retire_removed(%__MODULE__{opts: %{stop_removed: false}} = cluster), do: cluster

  defp retire_removed(cluster) do
    gone =
      for id <- Enum.sort(cluster.invariants.removed),
          Map.has_key?(cluster.members, id),
          up?(cluster.members, id),
          do: id

    Enum.reduce(gone, cluster, &retire(&2, &1))
  end

  defp retire(cluster, id) do
    store = cluster.members[id].store
    disk = Store.disk(store)
    retired = %{cluster | retired: MapSet.put(cluster.retired, id)}

    crash_member(retired, id, disk)
  end

  # The core hears how far the machine got and may ask for a snapshot
  defp feed_applied(cluster, id, effects) do
    indexes = for {:apply, entries, _refs} <- effects, do: List.last(entries).index

    Enum.reduce(indexes, cluster, fn index, acc ->
      if up?(acc.members, id), do: step_member(acc, id, {:applied, index}), else: acc
    end)
  end

  defp note_snapshot(cluster, id, before, effects) do
    installed = Enum.find(effects, &match?({:install_snapshot, _meta}, &1))
    node = cluster.members[id].node

    case installed do
      {:install_snapshot, meta} when node != nil ->
        checked = Invariants.snapshot_installed(cluster.invariants, id, node.log, meta)
        counted = %{cluster | invariants: checked}
        truncated? = Enum.any?(effects, &match?({:truncate_from, _index}, &1))

        count_suffix(counted, node, before, meta, truncated?)

      _none ->
        cluster
    end
  end

  defp count_suffix(cluster, _node, before, _meta, true) do
    if before.log.last_index > before.commit,
      do: note(cluster, :log_suffix_dropped),
      else: cluster
  end

  defp count_suffix(cluster, node, _before, meta, false) do
    if node.log.last_index > meta.index, do: note(cluster, :log_suffix_kept), else: cluster
  end

  # One batch per step reaches the page cache. The sync comes later
  defp write_disk(cluster, id, effects) do
    ops = Enum.flat_map(effects, &disk_ops/1)
    seqs = for {:persist, seq, _hard_state, _entries} <- effects, do: seq

    hard_states =
      for {:persist, _seq, %HardState{} = hard_state, _entries} <- effects, do: hard_state

    waiting = cluster.members[id].syncing
    syncing = queued(waiting, seqs, hard_states)

    if ops == [] and seqs == [],
      do: cluster,
      else: write_batch(cluster, id, ops, syncing)
  end

  defp write_batch(cluster, id, ops, syncing) do
    member = cluster.members[id]

    case Store.write(member.store, ops) do
      {:ok, store} -> put_member(cluster, id, %{member | store: store, syncing: syncing})
      {:error, failed, _reason} -> crash_failed(cluster, id, failed)
    end
  end

  defp crash_failed(cluster, id, store) do
    disk = Store.disk(store)

    crash_member(cluster, id, disk)
  end

  defp queued(nil, seqs, hard_states), do: %{seq: List.last(seqs), hard_states: hard_states}

  defp queued(%{seq: seq, hard_states: waiting}, seqs, hard_states) do
    last = List.last(seqs)

    %{seq: newest_seq(last, seq), hard_states: waiting ++ hard_states}
  end

  defp newest_seq(nil, seq), do: seq
  defp newest_seq(last, _seq), do: last

  defp disk_ops({:truncate_from, index}), do: [{:truncate_from, index}]
  defp disk_ops({:compact_log, keep_from}), do: [{:compact_log, keep_from}]

  defp disk_ops({:persist, _seq, hard_state, entries}) do
    hard_state_ops = if hard_state, do: [{:hard_state, hard_state}], else: []
    entry_ops = if entries == [], do: [], else: [{:entries, entries}]

    entry_ops ++ hard_state_ops
  end

  defp disk_ops(_effect), do: []

  defp apply_effect(cluster, _id, _before, {:persist, _seq, _hard_state, _entries}), do: cluster

  defp apply_effect(cluster, _id, _before, {:compact_log, _keep_from}), do: cluster

  defp apply_effect(cluster, id, _before, {:take_snapshot, index}) do
    member = cluster.members[id]

    put_member(cluster, id, %{member | snapshots: Enum.concat(member.snapshots, [{:take, index}])})
  end

  defp apply_effect(cluster, id, _before, {:send_snapshot, peer, meta, term, seq}) do
    member = cluster.members[id]

    restarted =
      if Map.has_key?(member.senders, peer),
        do: note(cluster, :snapshot_send_restarted),
        else: cluster

    start_sender(restarted, id, peer, meta, term, seq)
  end

  defp apply_effect(cluster, id, _before, {:snapshot_ack, peer, offset}) do
    case Map.fetch(cluster.members[id].senders, peer) do
      {:ok, sender} ->
        send_chunks(cluster, id, peer, %{
          sender
          | stream: SnapshotStream.ack(sender.stream, offset)
        })

      :error ->
        cluster
    end
  end

  defp apply_effect(cluster, id, _before, {:cancel_snapshot_sends}) do
    member = cluster.members[id]

    put_member(cluster, id, %{member | senders: %{}})
  end

  defp apply_effect(cluster, id, _before, {:store_snapshot_chunk, meta, offset, data, done?}),
    do: store_chunk(cluster, id, meta, offset, data, done?)

  defp apply_effect(cluster, id, _before, {:install_snapshot, meta}) do
    case install_file(cluster, id, meta) do
      {:ok, installed} ->
        note(installed, :snapshot_installed)

      {:error, reason} ->
        %{cluster | invariants: Invariants.recovery_failed(cluster.invariants, id, reason)}
    end
  end

  defp apply_effect(cluster, _id, _before, {:truncate_from, _index}),
    do: note(cluster, :conflict_truncation)

  defp apply_effect(
         cluster,
         _id,
         _before,
         {:send, %RequestVoteReply{pre_vote?: true, granted?: false} = message}
       ) do
    noted = note(cluster, :pre_vote_refused)

    %{noted | network: Network.enqueue(noted.network, message)}
  end

  defp apply_effect(cluster, id, _before, {:send, %RequestVoteReply{granted?: true} = message}) do
    granted = if restarted?(cluster, id), do: note(cluster, :vote_after_restart), else: cluster

    %{granted | network: Network.enqueue(granted.network, message)}
  end

  defp apply_effect(cluster, id, _before, {:send, %ReadIndexReply{ok?: true} = message}) do
    checked = check_forwarded_read(cluster, id, message)
    noted = note(checked, :forwarded_read)

    %{noted | network: Network.enqueue(noted.network, message)}
  end

  defp apply_effect(cluster, _id, _before, {:send, message}),
    do: %{cluster | network: Network.enqueue(cluster.network, message)}

  defp apply_effect(cluster, id, _before, {:apply, entries, refs}) do
    {applied, results} = Enum.reduce(entries, {cluster, %{}}, &apply_one(&1, &2, id))
    answered = queue_results(applied, refs, results)

    if noop_commit?(answered.members[id].node, entries),
      do: note(answered, :noop_commit),
      else: answered
  end

  defp apply_effect(cluster, _id, _before, {:reply, ref, {:error, error}}) do
    noted = note_refusal(cluster, Map.get(cluster.refs, ref), error)

    queue_reply(noted, ref, {:refused, error})
  end

  defp apply_effect(cluster, _id, _before, {:transferred, _ref, _target}),
    do: note(cluster, :transfer_finished)

  defp apply_effect(cluster, id, _before, {:read_ready, ref, index}) do
    member = cluster.members[id]
    {held, reads} = Map.pop(member.reads, ref)

    case held do
      nil ->
        cluster

      {query, newest} ->
        checked = check_leader_read(cluster, id, newest)
        waits = Enum.concat(member.read_waits, [{index, ref, query}])

        put_member(checked, id, %{member | reads: reads, read_waits: waits})
    end
  end

  defp apply_effect(cluster, id, _before, {:role_changed, :leader, term, id}) do
    invariants = Invariants.leader_elected(cluster.invariants, id, term)
    node = cluster.members[id].node
    checked = Invariants.leader_log(invariants, id, term, node.log)

    note(%{cluster | invariants: checked}, :leader_elected)
  end

  defp apply_effect(
         cluster,
         id,
         %Node{role: :leader} = before,
         {:role_changed, _role, _term, _leader}
       ) do
    stopped = note(cluster, :leader_stepped_down)
    node = cluster.members[id].node

    cond do
      not Config.member?(node.config, id) -> note(stopped, :self_removed_leader_stepped_down)
      node.term == before.term -> note(stopped, :check_quorum_step_down)
      true -> stopped
    end
  end

  defp apply_effect(cluster, _id, _before, {:role_changed, _role, _term, _leader}), do: cluster

  # A new leader commits older entries together with the no-op of its own term
  defp noop_commit?(%Node{role: :leader, term: term}, entries) do
    Enum.any?(entries, &(&1.kind == :noop and &1.term == term)) and
      Enum.any?(entries, &(&1.kind == :command and &1.term < term))
  end

  defp noop_commit?(%Node{}, _entries), do: false

  defp check_durable(cluster) do
    logs = Map.new(cluster.ids, &{&1, durable_log(cluster, &1)})
    quorum = div(length(cluster.ids), 2) + 1
    invariants = Invariants.acknowledged(cluster.invariants, logs, quorum)

    %{cluster | invariants: invariants}
  end

  # What a member recovers if it crashes now. Every unsynced write is lost.
  defp durable_log(cluster, id) do
    member = cluster.members[id]
    disk = if member.store, do: Store.disk(member.store), else: member.disk
    strict = SimDisk.crash(%{disk | profile: :strict})
    path = dir(id)
    store_opts = store_opts(cluster, id)

    case Store.read_log(strict, path, store_opts) do
      {:ok, _disk, %Result{entries: entries} = result} ->
        held = Map.new(entries, &{&1.index, {&1.term, &1.data}})

        covered = covered(cluster, result)

        {:ok, Map.merge(covered, held)}

      {:error, _disk, reason} ->
        {:error, reason}
    end
  end

  # A durable snapshot holds every committed entry up to its index
  defp covered(_cluster, %Result{snapshot: nil}), do: %{}

  defp covered(cluster, %Result{snapshot: %SnapshotMeta{index: index}}) do
    for {at, seen} <- cluster.invariants.committed, at <= index, into: %{}, do: {at, seen}
  end

  defp apply_one(entry, {cluster, results}, id) do
    {applied, result} = apply_entry(cluster, id, entry)

    {applied, Map.put(results, entry.index, result)}
  end

  defp apply_entry(cluster, id, %Entry{} = entry) do
    member = cluster.members[id]
    module = machine_module(cluster)

    {state, result} = module.apply(member.machine, entry)

    stored = %{
      member
      | machine: state,
        applied: entry.index,
        applied_term: entry.term,
        applied_time_ms: entry.time_ms,
        config: config_after(member.config, entry)
    }

    stored_cluster = put_member(cluster, id, stored)

    {check_applied(stored_cluster, id, entry, state, result), result}
  end

  defp config_after(_held, %Entry{kind: :config, data: data}), do: Config.decode!(data)
  defp config_after(held, %Entry{}), do: held

  defp check_applied(%__MODULE__{opts: %{workload: :chain}} = cluster, id, entry, state, _result) do
    invariants =
      cluster
      |> noted_commit(id, entry)
      |> Invariants.applied(id, entry)
      |> Invariants.chained(id, state)

    %{cluster | invariants: invariants}
  end

  defp check_applied(cluster, id, entry, state, result) do
    applied = cluster |> noted_commit(id, entry) |> Invariants.applied(id, entry)
    resulted = Invariants.result_applied(applied, id, entry.index, result)

    module = machine_module(cluster)

    %{cluster | invariants: digest_state(resulted, id, entry.index, module, state)}
  end

  defp noted_commit(cluster, id, entry) do
    node = cluster.members[id].node
    committed = Invariants.committed_in(cluster.invariants, entry.index, node.term)
    checked = Invariants.config_applied(committed, id, entry)

    Invariants.committed_with(checked, entry.index, node.config)
  end

  # A whole state is hashed at every sixteenth index only. A result is checked at each
  defp digest_state(invariants, id, index, module, state) when rem(index, 16) == 0 do
    digest = :crypto.hash(:sha256, module.snapshot(state))

    Invariants.state_applied(invariants, id, index, digest)
  end

  defp digest_state(invariants, _id, _index, _module, _state), do: invariants

  defp machine_module(%__MODULE__{opts: %{workload: :chain}}), do: ChainMachine
  defp machine_module(%__MODULE__{opts: %{bug: bug}}) when bug in @kv_bugs, do: FaultyKv
  defp machine_module(%__MODULE__{}), do: KvMachine

  defp node_bug(bug) when bug in @kv_bugs, do: nil
  defp node_bug(bug), do: bug

  defp init_machine(%__MODULE__{opts: %{workload: :chain}}), do: ChainMachine.init(%{})

  defp init_machine(%__MODULE__{opts: opts} = cluster) do
    config = %{
      session_ttl_ms: opts.session_ttl_ms,
      max_sessions: opts.max_sessions,
      session_sweep_interval: 16,
      bug: opts.bug
    }

    machine_module(cluster).init(config)
  end

  # Snapshots

  defp snapshot_work(cluster, id, {:take, _index}) do
    member = cluster.members[id]
    module = machine_module(cluster)
    payload = module.snapshot(member.machine)
    disk = Store.disk(member.store)

    written =
      Snapshot.write(
        disk,
        snap_dir(id),
        member.applied,
        member.applied_term,
        member.applied_time_ms,
        payload,
        member.config
      )

    case written do
      {:ok, next, meta} -> taken(cluster, id, next, meta, module)
      {:error, next, _reason} -> step_member(with_disk(cluster, id, next), id, :snapshot_failed)
    end
  end

  defp snapshot_work(cluster, id, {:received, meta, bytes}) do
    member = cluster.members[id]
    disk = Store.disk(member.store)

    with :ok <- Snapshot.verify(bytes, meta),
         {:ok, next} <- Snapshot.store(disk, snap_dir(id), bytes, meta) do
      step_member(with_disk(cluster, id, next), id, {:snapshot_received, meta})
    else
      _failed -> cluster
    end
  end

  defp taken(cluster, id, disk, meta, module) do
    stored = with_disk(cluster, id, disk)
    member = stored.members[id]
    checked = check_snapshot(stored.invariants, stored, id, meta.index, module, member.machine)
    counted = note(%{stored | invariants: checked}, :snapshot_taken)

    step_member(counted, id, {:snapshot_taken, meta})
  end

  defp with_disk(cluster, id, disk) do
    member = cluster.members[id]

    put_member(cluster, id, %{member | store: %{member.store | disk: disk}})
  end

  # The file is read once. Chunks leave as the follower acknowledges what it holds
  defp start_sender(cluster, id, peer, meta, term, seq) do
    member = cluster.members[id]
    disk = Store.disk(member.store)
    dir = snap_dir(id)
    path = Snapshot.path(dir, meta)

    case SimDisk.read_file(disk, path) do
      {:ok, _disk, data} -> begin_send(cluster, id, peer, meta, term, seq, data)
      {:error, _disk, _reason} -> cluster
    end
  end

  defp begin_send(cluster, id, peer, meta, term, seq, data) do
    opts = cluster.opts

    stream =
      SnapshotStream.new(
        meta: meta,
        term: term,
        seq: seq,
        from: id,
        to: peer,
        chunk_bytes: opts.snapshot_chunk_bytes,
        max_inflight: opts.snapshot_max_inflight
      )

    send_chunks(cluster, id, peer, %{stream: stream, data: data})
  end

  defp send_chunks(cluster, id, peer, %{stream: stream, data: data}) do
    {pulled, ranges} = SnapshotStream.pull(stream)
    sender = %{stream: pulled, data: data}
    member = cluster.members[id]
    kept = put_member(cluster, id, %{member | senders: Map.put(member.senders, peer, sender)})

    Enum.reduce(ranges, kept, fn {offset, length}, acc ->
      chunk = binary_part(data, offset, length)
      message = SnapshotStream.message(pulled, offset, chunk)

      %{acc | network: Network.enqueue(acc.network, message)}
    end)
  end

  # Chunks arrive in order or restart at zero. The core answers every chunk itself
  defp store_chunk(cluster, id, meta, 0, data, done?),
    do: hold_chunk(cluster, id, %{meta: meta, data: data}, done?)

  defp store_chunk(cluster, id, meta, offset, data, done?) do
    case cluster.members[id].receiving do
      %{meta: ^meta, data: held} when byte_size(held) == offset ->
        hold_chunk(cluster, id, %{meta: meta, data: held <> data}, done?)

      _other ->
        cluster
    end
  end

  defp hold_chunk(cluster, id, receiving, true) do
    member = cluster.members[id]
    work = {:received, receiving.meta, receiving.data}

    put_member(cluster, id, %{
      member
      | receiving: nil,
        snapshots: Enum.concat(member.snapshots, [work])
    })
  end

  defp hold_chunk(cluster, id, receiving, false) do
    member = cluster.members[id]

    put_member(cluster, id, %{member | receiving: receiving})
  end

  # Clients and their commands

  defp new_clients(%{workload: :kv, clients: count}), do: Map.new(1..count, &{&1, Client.new(&1)})
  defp new_clients(_config), do: %{}

  defp ready_slots(cluster) do
    for {slot, client} <- Enum.sort(cluster.clients), Client.ready?(client), do: slot
  end

  # Once the cluster commits again every client gets a few turns to write and read
  defp heal_clients(cluster) do
    if quota_unmet?(cluster) and cluster.heal.committed?,
      do: cluster.clients |> Map.keys() |> Enum.sort(),
      else: pending_clients(cluster)
  end

  defp quota_unmet?(%__MODULE__{opts: %{workload: :kv}} = cluster) do
    started(cluster) - cluster.heal.base < @heal_ops * map_size(cluster.clients)
  end

  defp quota_unmet?(%__MODULE__{}), do: false

  defp started(cluster), do: cluster.clients |> Map.values() |> Enum.map(& &1.seq) |> Enum.sum()

  defp pending_clients(cluster) do
    for {slot, client} <- Enum.sort(cluster.clients), Client.pending?(client), do: slot
  end

  defp join_round(cluster, slot, targets, number) do
    target = Enum.at(targets, slot - 1)

    perform(cluster, {:client, slot, {:compare_and_set, {:round, number}, target}})
  end

  defp run_outputs(cluster, slot, outputs),
    do: Enum.reduce(outputs, cluster, &run_output(&2, slot, &1))

  defp run_output(cluster, _slot, {:event, event}),
    do: %{cluster | history: [event | cluster.history]}

  defp run_output(cluster, slot, {:send, target, data, seq}),
    do: send_command(cluster, target, data, slot, seq)

  defp run_output(cluster, slot, {:query, target, query, seq}),
    do: send_query(cluster, target, query, slot, seq)

  # A member that is down gives no answer
  defp send_command(cluster, target, data, slot, seq) do
    if active?(cluster.members, target) do
      ref = make_ref()
      sent = %{cluster | refs: Map.put(cluster.refs, ref, {slot, seq})}
      time_ms = wall_ms(sent, target)

      step_member(sent, target, {:propose, ref, data, time_ms})
    else
      cluster
    end
  end

  # A read waits in the member until the core confirms it and the machine catches up
  defp send_query(cluster, target, query, slot, seq) do
    if active?(cluster.members, target) do
      ref = make_ref()
      member = cluster.members[target]
      newest = cluster.invariants.newest_commit_term
      reads = Map.put(member.reads, ref, {query, newest})
      refs = Map.put(cluster.refs, ref, {slot, seq})
      sent = put_member(%{cluster | refs: refs}, target, %{member | reads: reads})

      step_member(sent, target, {:read_index, ref})
    else
      cluster
    end
  end

  defp drain_reads(cluster, id) do
    member = cluster.members[id]

    if member.node == nil do
      cluster
    else
      {ready, waiting} =
        Enum.split_with(member.read_waits, fn {index, _ref, _query} -> index <= member.applied end)

      held = put_member(cluster, id, %{member | read_waits: waiting})

      Enum.reduce(ready, held, fn {_index, ref, query}, acc ->
        answer_read(acc, id, ref, query)
      end)
    end
  end

  defp answer_read(cluster, id, ref, query) do
    module = machine_module(cluster)
    result = module.query(cluster.members[id].machine, query)

    queue_reply(cluster, ref, {:result, result})
  end

  defp note_batch(cluster, effects) do
    served =
      Enum.count(effects, fn
        {:read_ready, _ref, _index} -> true
        {:send, %ReadIndexReply{ok?: true}} -> true
        _other -> false
      end)

    if served >= 2, do: note(cluster, :batched_read), else: cluster
  end

  defp note_refusal(cluster, :change, :change_in_progress),
    do: note(cluster, :change_in_progress_refused)

  defp note_refusal(cluster, :change, :not_ready),
    do: note(cluster, :change_before_term_commit_refused)

  defp note_refusal(cluster, :change, _error), do: cluster
  defp note_refusal(cluster, _ref, :leadership_lost), do: note(cluster, :leadership_lost_reply)
  defp note_refusal(cluster, _ref, :timeout), do: note(cluster, :transfer_gave_up)
  defp note_refusal(cluster, _ref, _error), do: cluster

  defp note_change(cluster, id, before, change) do
    case cluster.members[id].node do
      %Node{} = node when node.log.last_index > before.log.last_index ->
        note_accepted(cluster, before, change)

      _other ->
        cluster
    end
  end

  defp note_accepted(cluster, _before, {:add_learner, _id}), do: note(cluster, :learner_added)
  defp note_accepted(cluster, _before, {:promote, _id}), do: note(cluster, :learner_promoted)

  defp note_accepted(cluster, before, {:remove, id}) do
    if Config.voter?(before.config, id), do: note(cluster, :voter_removed), else: cluster
  end

  defp check_leader_read(cluster, id, newest) do
    node = cluster.members[id].node

    if node.role == :leader,
      do: confirm_read(cluster, id, node.term, newest),
      else: cluster
  end

  defp check_forwarded_read(cluster, id, %ReadIndexReply{to: follower, ref_id: ref_id}) do
    node = cluster.members[follower].node
    held = held_forward(node, ref_id)

    case held do
      {ref, _tick} -> forwarded_start(cluster, id, follower, ref)
      nil -> cluster
    end
  end

  defp held_forward(nil, _ref_id), do: nil
  defp held_forward(node, ref_id), do: Map.get(node.forwards, ref_id)

  defp forwarded_start(cluster, id, follower, ref) do
    case Map.get(cluster.members[follower].reads, ref) do
      {_query, newest} -> confirm_read(cluster, id, cluster.members[id].node.term, newest)
      nil -> cluster
    end
  end

  defp confirm_read(cluster, id, term, newest),
    do: %{cluster | invariants: Invariants.read_served(cluster.invariants, id, term, newest)}

  defp queue_results(cluster, refs, results) do
    refs
    |> Enum.sort()
    |> Enum.reduce(cluster, fn {index, ref}, acc ->
      queue_reply(acc, ref, {:result, Map.fetch!(results, index)})
    end)
  end

  # A reply waits until the member is done with its step. A client may answer
  # by sending to the same member and that send must not cut into the step.
  defp queue_reply(cluster, ref, reply),
    do: %{cluster | replies: [{ref, reply} | cluster.replies]}

  defp deliver_replies(%__MODULE__{replies: []} = cluster), do: cluster

  defp deliver_replies(%__MODULE__{} = cluster) do
    waiting = Enum.reverse(cluster.replies)
    cleared = %{cluster | replies: []}

    Enum.reduce(waiting, cleared, fn {ref, reply}, acc -> answer(acc, ref, reply) end)
  end

  defp answer(cluster, ref, reply) do
    case Map.pop(cluster.refs, ref) do
      {nil, _refs} ->
        cluster

      {:change, refs} ->
        %{cluster | refs: refs}

      {{slot, seq}, refs} ->
        popped = %{cluster | refs: refs}
        {client, outputs} = Client.reply(popped.clients[slot], seq, reply, popped.now)
        replied = %{popped | clients: Map.put(popped.clients, slot, client)}

        run_outputs(replied, slot, outputs)
    end
  end

  # Each member reads the clock a little off. The skew never changes
  defp wall_ms(cluster, id) do
    spread = cluster.opts.skew_ms
    skew = :erlang.phash2({cluster.opts.seed, id, :skew}, 2 * spread + 1) - spread

    max(cluster.now + skew, 0)
  end

  defp closed_history(cluster) do
    closing =
      for {_slot, client} <- Enum.sort(cluster.clients),
          {_ended, outputs} <- [Client.finish(client, cluster.now)],
          {:event, event} <- outputs,
          do: event

    Enum.reverse(cluster.history) ++ closing
  end

  defp history_violations(%__MODULE__{opts: %{workload: :kv} = opts}, events) do
    ops = History.ops(events)

    KvCheck.check(ops, max_states: opts.check_max_states)
  end

  defp history_violations(%__MODULE__{}, _events), do: []

  defp put_member(cluster, id, member),
    do: %{cluster | members: Map.put(cluster.members, id, member)}

  defp random_bytes(rand, count) do
    {bytes, next} =
      Enum.map_reduce(1..count, rand, fn _position, state -> :rand.uniform_s(256, state) end)

    data = bytes |> Enum.map(&(&1 - 1)) |> :erlang.list_to_binary()

    {data, next}
  end

  defp extend_hash(cluster, action) do
    digests =
      Enum.map(cluster.ids, fn id ->
        if node = cluster.members[id].node, do: Node.digest(node), else: <<>>
      end)

    encoded = :erlang.term_to_binary(action)

    :crypto.hash(:sha256, [cluster.trace_hash, encoded | digests])
  end
end
