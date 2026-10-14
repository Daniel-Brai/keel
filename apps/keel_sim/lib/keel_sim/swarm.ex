defmodule KeelSim.Swarm do
  alias KeelSim.Disk

  @sizes [1, 3, 3, 3, 5, 5]

  @faults ~w[drop duplicate crash partition]a

  # Each timer holds the election range and the heartbeat
  @timers [{3, 6, 1}, {10, 20, 2}, {10, 20, 2}, {30, 60, 3}]

  @batches [1, 4, 16]

  @fault_rates [0, 20, 40]

  # One run in four holds the chain of blocks. The rest hold the key value store
  @workloads ~w[chain kv kv kv]a

  @client_counts [2, 3, 4]

  @session_ttls [1_000, 5_000, 60_000]

  @session_limits [4, 64, 64]

  # Small thresholds make members snapshot and compact often
  @snapshot_thresholds [150, 400, 1_000]

  @trailing_entries [0, 2, 8]

  @chunk_sizes [16, 64, 256]

  @inflight_chunks [1, 2, 4]

  @ack_timeouts [10, 30, 100]

  @install_timeouts [30, 90, 300]

  @snapshot_weights 10..30

  @weight_ranges %{
    tick: 30..50,
    propose: 4..16,
    deliver: 30..50,
    complete_persist: 20..40,
    drop: 1..6,
    duplicate: 1..6,
    crash: 1..4,
    restart: 2..8,
    isolate: 1..4,
    heal: 2..8
  }

  @pause_weights 1..3

  @resume_weights 1..4

  @transfer_weights 1..4

  @membership_weights 1..4

  @join_weights 1..4

  @client_weights 20..40

  @round_weights 2..6

  @type fault :: :drop | :duplicate | :crash | :partition | :pause | :transfer | :membership

  @type weights :: %{
          tick: pos_integer(),
          propose: pos_integer(),
          deliver: pos_integer(),
          complete_persist: pos_integer(),
          drop: pos_integer(),
          duplicate: pos_integer(),
          crash: pos_integer(),
          restart: pos_integer(),
          isolate: pos_integer(),
          heal: pos_integer(),
          pause: pos_integer(),
          resume: pos_integer(),
          transfer: pos_integer(),
          membership: pos_integer(),
          join: pos_integer(),
          client: pos_integer(),
          round: pos_integer(),
          snapshot_step: pos_integer()
        }

  @type workload :: :chain | :kv

  @type bug :: nil | Keel.Raft.Node.bug() | :kv_drop_put | :kv_no_dedupe

  @type config :: %{
          seed: integer(),
          size: pos_integer(),
          steps: non_neg_integer(),
          heal_steps: non_neg_integer(),
          faults: [fault()],
          weights: weights(),
          crash_profile: Disk.profile(),
          election_ticks_min: pos_integer(),
          election_ticks_max: pos_integer(),
          heartbeat_ticks: pos_integer(),
          max_append_entries: pos_integer(),
          fault_rate: non_neg_integer(),
          bug: bug(),
          segment_bytes: pos_integer(),
          workload: workload(),
          clients: pos_integer(),
          session_ttl_ms: pos_integer(),
          max_sessions: pos_integer(),
          check_max_states: pos_integer(),
          skew_ms: non_neg_integer(),
          pre_vote: boolean(),
          bootstrap: boolean(),
          trace: boolean(),
          check_acks: boolean(),
          stop_removed: boolean(),
          promote_lag_entries: non_neg_integer(),
          snapshot_threshold_bytes: non_neg_integer(),
          snapshot_trailing_entries: non_neg_integer(),
          snapshot_chunk_bytes: pos_integer(),
          snapshot_max_inflight: pos_integer(),
          snapshot_ack_timeout_ticks: pos_integer(),
          snapshot_install_timeout_ticks: pos_integer()
        }

  @spec resolve(map()) :: config()
  def resolve(%{seed: seed} = opts) do
    explicit = opts |> without_faults_flag() |> Map.drop([:weights])
    given = Map.get(opts, :weights, %{})
    drawn = draw(seed)
    weights = Map.merge(drawn.weights, given)

    defaults = %{
      steps: 5_000,
      heal_steps: 6_000,
      bug: nil,
      segment_bytes: 2_048,
      check_max_states: 300_000,
      skew_ms: 200,
      bootstrap: true,
      trace: false,
      check_acks: true,
      stop_removed: true,
      promote_lag_entries: 6
    }

    drawn
    |> Map.merge(defaults)
    |> Map.merge(explicit)
    |> Map.put(:weights, weights)
  end

  @spec draw(integer()) :: map()
  def draw(seed) do
    profiles = Disk.profiles()
    start = :rand.seed_s(:exsss, {seed, 17, 31})
    {size, after_size} = pick(@sizes, start)
    {faults, after_faults} = subset(@faults, after_size)
    {weights, after_weights} = draw_weights(after_faults)
    {profile, after_profile} = pick(profiles, after_weights)
    {{min, max, heartbeat}, after_timers} = pick(@timers, after_profile)
    {batch, after_batch} = pick(@batches, after_timers)
    {fault_rate, _rest} = pick(@fault_rates, after_batch)

    workload = draw_workload(seed)
    snapshot = draw_snapshot(seed)
    extras = draw_extras(seed)

    all_weights =
      weights
      |> Map.merge(workload.weights)
      |> Map.merge(snapshot.weights)
      |> Map.merge(extras.weights)

    %{
      seed: seed,
      size: size,
      faults: faults ++ extras.faults,
      weights: all_weights,
      pre_vote: extras.pre_vote,
      workload: workload.workload,
      clients: workload.clients,
      session_ttl_ms: workload.session_ttl_ms,
      max_sessions: workload.max_sessions,
      crash_profile: profile,
      election_ticks_min: min,
      election_ticks_max: max,
      heartbeat_ticks: heartbeat,
      max_append_entries: batch,
      fault_rate: fault_rate,
      snapshot_threshold_bytes: snapshot.threshold,
      snapshot_trailing_entries: snapshot.trailing,
      snapshot_chunk_bytes: snapshot.chunk,
      snapshot_max_inflight: snapshot.inflight,
      snapshot_ack_timeout_ticks: snapshot.ack_timeout,
      snapshot_install_timeout_ticks: snapshot.install_timeout
    }
  end

  # Its own stream so that adding a dimension never moves the draws before it
  defp draw_extras(seed) do
    start = :rand.seed_s(:exsss, {seed, 71, 73})
    {pre_vote, after_pre_vote} = pick(~w[true true true false]a, start)
    {pausing, after_pausing} = pick([true, false], after_pre_vote)
    {transferring, after_transferring} = pick(~w[true true false]a, after_pausing)
    {pause, after_pause} = between(@pause_weights, after_transferring)
    {resume, after_resume} = between(@resume_weights, after_pause)
    {transfer, after_transfer} = between(@transfer_weights, after_resume)
    {changing, after_changing} = pick(~w[true true false]a, after_transfer)
    {membership, after_membership} = between(@membership_weights, after_changing)
    {join, _rest} = between(@join_weights, after_membership)

    pause_faults = enabled(:pause, pausing)
    transfer_faults = enabled(:transfer, transferring)
    membership_faults = enabled(:membership, changing)
    faults = pause_faults ++ transfer_faults ++ membership_faults

    %{
      pre_vote: pre_vote,
      faults: faults,
      weights: %{
        pause: pause,
        resume: resume,
        transfer: transfer,
        membership: membership,
        join: join
      }
    }
  end

  defp draw_snapshot(seed) do
    start = :rand.seed_s(:exsss, {seed, 61, 67})
    {threshold, after_threshold} = pick(@snapshot_thresholds, start)
    {trailing, after_trailing} = pick(@trailing_entries, after_threshold)
    {chunk, after_chunk} = pick(@chunk_sizes, after_trailing)
    {inflight, after_inflight} = pick(@inflight_chunks, after_chunk)
    {ack_timeout, after_ack} = pick(@ack_timeouts, after_inflight)
    {install_timeout, after_install} = pick(@install_timeouts, after_ack)
    {step, _rest} = between(@snapshot_weights, after_install)

    %{
      threshold: threshold,
      trailing: trailing,
      chunk: chunk,
      inflight: inflight,
      ack_timeout: ack_timeout,
      install_timeout: install_timeout,
      weights: %{snapshot_step: step}
    }
  end

  # Its own stream so that adding a dimension never moves the draws before it
  defp draw_workload(seed) do
    start = :rand.seed_s(:exsss, {seed, 53, 59})
    {workload, after_workload} = pick(@workloads, start)
    {clients, after_clients} = pick(@client_counts, after_workload)
    {ttl, after_ttl} = pick(@session_ttls, after_clients)
    {sessions, after_sessions} = pick(@session_limits, after_ttl)
    {client, after_client} = between(@client_weights, after_sessions)
    {round, _rest} = between(@round_weights, after_client)

    %{
      workload: workload,
      clients: clients,
      session_ttl_ms: ttl,
      max_sessions: sessions,
      weights: %{client: client, round: round}
    }
  end

  defp between(first..last//_step, rand) do
    {draw, next} = :rand.uniform_s(last - first + 1, rand)

    {first + draw - 1, next}
  end

  defp without_faults_flag(%{faults?: false} = opts),
    do: opts |> Map.delete(:faults?) |> Map.put(:faults, []) |> Map.put(:fault_rate, 0)

  defp without_faults_flag(opts), do: Map.delete(opts, :faults?)

  defp pick(list, rand) do
    count = length(list)
    {at, next} = :rand.uniform_s(count, rand)

    {Enum.at(list, at - 1), next}
  end

  defp subset(list, rand) do
    {kept, next} =
      Enum.reduce(list, {[], rand}, fn item, {acc, state} ->
        {draw, after_draw} = :rand.uniform_s(2, state)

        if draw == 1, do: {[item | acc], after_draw}, else: {acc, after_draw}
      end)

    {Enum.reverse(kept), next}
  end

  defp draw_weights(rand) do
    {pairs, next} =
      @weight_ranges
      |> Enum.sort()
      |> Enum.map_reduce(rand, fn {kind, first..last//_step}, state ->
        {draw, after_draw} = :rand.uniform_s(last - first + 1, state)

        {{kind, first + draw - 1}, after_draw}
      end)

    {Map.new(pairs), next}
  end

  defp enabled(fault, true), do: [fault]
  defp enabled(_fault, false), do: []
end
