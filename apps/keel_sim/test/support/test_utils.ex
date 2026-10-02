defmodule KeelSim.TestUtils do
  use ExUnitProperties

  alias Keel.Log.Recovery.Result
  alias Keel.Log.Snapshot
  alias Keel.Log.Store
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.Log
  alias Keel.Raft.SnapshotMeta
  alias KeelSim.Client
  alias KeelSim.Client.Op
  alias KeelSim.Disk
  alias KeelSim.Purity

  def impure_helper_fixture do
    """
    defmodule Keel.PurityFixture.Clock do
      def now, do: System.monotonic_time()
    end

    defmodule Keel.Raft.PurityFixture do
      def stamp(event), do: {event, Keel.PurityFixture.Clock.now()}
    end
    """
  end

  def compiled_imports(source) do
    source
    |> Code.compile_string()
    |> Enum.map(fn {_module, beam} -> Purity.imports(beam) end)
  end

  def compiled_app_imports(app) do
    app
    |> Application.app_dir("ebin")
    |> Path.join("*.beam")
    |> Path.wildcard()
    |> Enum.map(&(&1 |> String.to_charlist() |> Purity.imports()))
  end

  def violating_seeds(seeds, opts) do
    Enum.filter(seeds, fn seed -> opts |> Map.put(:seed, seed) |> KeelSim.run() |> violated?() end)
  end

  def first_violation(seeds, opts) do
    Enum.find_value(seeds, fn seed ->
      result = opts |> Map.put(:seed, seed) |> KeelSim.run()

      if violated?(result), do: {seed, result.violations}
    end)
  end

  def swept(seeds, opts) do
    seeds
    |> Task.async_stream(&run_seed(opts, &1), timeout: :infinity)
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp run_seed(opts, seed) do
    seeded = Map.put(opts, :seed, seed)

    KeelSim.run(seeded)
  end

  def self_overlaps(ops) do
    for first <- ops,
        second <- ops,
        first.process == second.process,
        first.invoked_at < second.invoked_at,
        first.status == :ok,
        first.completed_at > second.invoked_at do
      {first, second}
    end
  end

  def shortest_failing_run(seeds, opts) do
    results = swept(seeds, opts)
    failing = Enum.filter(results, &violated?/1)

    Enum.min_by(failing, &length(&1.schedule), fn -> nil end)
  end

  def liveness_failures(seeds, opts) do
    results = swept(seeds, opts)

    for result <- results, result.liveness != [], do: result.config.seed
  end

  def failing_seeds(seeds, opts) do
    results = swept(seeds, opts)

    for result <- results, violated?(result) or result.liveness != [] do
      result.config.seed
    end
  end

  def coverage_after(seeds, opts) do
    results = swept(seeds, opts)
    empty = KeelSim.Coverage.new()

    Enum.reduce(results, empty, &KeelSim.Coverage.merge(&2, &1.coverage))
  end

  def random_config(opts \\ []) do
    voters = Keyword.get(opts, :voters, ~w[m1 m2 m3])
    learners = Keyword.get(opts, :learners, [])
    removed = Keyword.get(opts, :removed, [])

    %Config{
      voters: MapSet.new(voters),
      learners: MapSet.new(learners),
      removed: MapSet.new(removed),
      addresses: %{}
    }
  end

  def config_entry(index, term, config),
    do: %Entry{
      index: index,
      term: term,
      kind: :config,
      data: Config.encode(config),
      time_ms: index
    }

  def unreached(coverage, names),
    do: for(name <- names, Map.fetch!(coverage, name) == 0, do: name)

  def grown_seeds(seeds, opts) do
    for seed <- seeds, grown?(opts, seed), do: seed
  end

  defp grown?(opts, seed) do
    seeded = Map.put(opts, :seed, seed)
    cluster = seeded |> KeelSim.Cluster.new() |> KeelSim.Cluster.chaos()

    length(cluster.ids) > length(cluster.founders)
  end

  def caught_seeds(seeds, opts), do: Enum.filter(seeds, &caught?(opts, &1))

  def loss_seeds(seeds, opts), do: Enum.filter(seeds, &lost?(opts, &1))

  defp caught?(opts, seed) do
    opts |> Map.put(:seed, seed) |> KeelSim.run() |> violated?()
  rescue
    Keel.Raft.InvariantError -> true
  end

  defp lost?(opts, seed) do
    result = opts |> Map.put(:seed, seed) |> KeelSim.run()

    Enum.any?(result.violations, &match?({:acknowledged_loss, _index}, &1))
  rescue
    Keel.Raft.InvariantError -> false
  end

  def disk_with_file!(profile, seed) do
    empty = Disk.new(profile: profile, seed: {seed, 2, 3})
    {:ok, made} = Disk.create_dir(empty, "/data")
    {:ok, opened, handle} = Disk.open(made, "/data/file", ~w[write create exclusive]a)

    {opened, handle}
  end

  def disk_with_pending_write!(profile, seed) do
    {disk, handle} = disk_with_file!(profile, seed)
    {:ok, written} = Disk.write_at(disk, handle, 0, "aaaa")
    {:ok, synced} = Disk.sync(written, handle)
    {:ok, named} = Disk.sync_dir(synced, "/data")
    {:ok, pending} = Disk.write_at(named, handle, 4, "bbbbbbbb")

    pending
  end

  def file_after_crash(disk) do
    crashed = Disk.crash(disk)

    case Disk.read_file(crashed, "/data/file") do
      {:ok, _disk, bytes} -> bytes
      {:error, _disk, :enoent} -> nil
    end
  end

  def torn_write_seen?(seeds) do
    Enum.any?(seeds, fn seed ->
      bytes = :torn |> disk_with_pending_write!(seed) |> file_after_crash()

      byte_size(bytes) > 4 and bytes != "aaaabbbbbbbb"
    end)
  end

  def unsynced_store! do
    {store, _result} = open_sim_store!(Disk.new())
    first = entries([{1, 1}])

    write!(store, [{:entries, first}])
  end

  def fail_next!(store, fault) do
    disk = Store.disk(store)
    faulty = Disk.inject(disk, fault)

    %{store | disk: faulty}
  end

  def open_sim_store!(disk, opts \\ []) do
    {:ok, store, result} = Store.open(disk, "/sim/log", opts)

    {store, result}
  end

  def crash_and_reopen!(store, opts \\ []) do
    {_store, result} = store |> Store.disk() |> Disk.crash() |> open_sim_store!(opts)

    result
  end

  def snapshotted_log(index, term) do
    meta = %SnapshotMeta{index: index, term: term, time_ms: 0, size: 1, checksum: 1}

    Log.reset(%Log{}, meta)
  end

  def snapshot_dir, do: "/sim/snap"

  def many_entries(from, last, term), do: entries(Enum.map(from..last, &{&1, term}))

  def store_with_entries!(entries, opts \\ []) do
    disk = Disk.new(profile: :strict)
    {store, _result} = open_sim_store!(disk, Keyword.merge([segment_bytes: 400], opts))

    Enum.reduce(entries, store, &write_synced!(&2, [{:entries, [&1]}]))
  end

  def snapshot!(store, index, term) do
    disk = Store.disk(store)
    {:ok, written, meta} = Snapshot.write(disk, snapshot_dir(), index, term, index * 10, "state")

    {%{store | disk: written}, meta}
  end

  def compact_synced!(store, keep_from) do
    synced = write_synced!(store, [{:compact_log, keep_from}])
    {:ok, removed} = Store.remove_compacted(synced)

    removed
  end

  def compact_unsynced!(store, keep_from), do: write!(store, [{:compact_log, keep_from}])

  # The batch is flushed but only the first of the old segments is removed before the crash
  def compact_halfway!(store, keep_from) do
    written = write!(store, [{:compact_log, keep_from}])
    disk = Store.disk(written)
    {:ok, flushed} = Disk.sync(disk, written.handle)
    {:ok, deleted} = Disk.delete(flushed, "/sim/log/00000001.seg")
    {:ok, synced} = Disk.sync_dir(deleted, "/sim/log")

    %{written | disk: synced}
  end

  def segment_count(store) do
    {:ok, _disk, names} = store |> Store.disk() |> Disk.crash() |> Disk.list("/sim/log")

    length(names)
  end

  def write_synced!(store, ops) do
    {:ok, written} = Store.write(store, ops)
    {:ok, synced} = Store.sync(written)

    synced
  end

  def write!(store, ops) do
    {:ok, written} = Store.write(store, ops)

    written
  end

  def entries(pairs) do
    Enum.map(pairs, fn {index, term} ->
      %Entry{
        index: index,
        term: term,
        kind: :command,
        data: :binary.copy(<<index>>, 30),
        time_ms: index
      }
    end)
  end

  def hard_state(term, voted_for), do: %HardState{term: term, voted_for: voted_for, commit: 0}

  def indexes(%Result{entries: entries}), do: Enum.map(entries, & &1.index)

  def crash_violations(profile, seeds), do: Enum.reject(seeds, &crash_consistent?(profile, &1))

  defp crash_consistent?(profile, seed) do
    rand = :rand.seed_s(:exsss, {seed, 5, 7})

    disk = Disk.new(profile: profile, seed: {seed, 1, 2})
    {opened, _result} = open_sim_store!(disk, segment_bytes: 512)

    start = {opened, [model(0)], 0, rand}

    {store, states, synced, _rand} = Enum.reduce(1..14, start, &random_batch/2)

    result = crash_and_reopen!(store, segment_bytes: 512)
    pairs = Enum.map(result.entries, &{&1.index, &1.term})
    recovered = {pairs, result.hard_state.term}

    states |> Enum.reverse() |> Enum.drop(synced) |> Enum.any?(&(&1 == recovered))
  end

  defp model(term), do: {[], term}

  defp random_batch(_step, {store, [{entries, term} | _] = states, synced, rand}) do
    {kind, after_kind} = :rand.uniform_s(4, rand)
    {size, next_rand} = :rand.uniform_s(3, after_kind)
    last = length(entries)

    {op, next} = batch_op(kind, entries, term, size, last)
    {:ok, written} = Store.write(store, [op])
    history = [next | states]

    if kind == 4 do
      {:ok, synced_store} = Store.sync(written)

      {synced_store, history, length(history) - 1, next_rand}
    else
      {written, history, synced, next_rand}
    end
  end

  defp batch_op(1, entries, term, size, last) do
    added = Enum.map((last + 1)..(last + size), &{&1, term + 1})

    {{:entries, entries(added)}, {entries ++ added, term}}
  end

  defp batch_op(2, entries, term, _size, last) when last > 0 do
    {{:truncate_from, last}, {Enum.take(entries, last - 1), term}}
  end

  defp batch_op(3, entries, term, _size, _last) do
    {{:hard_state, hard_state(term + 1, "m1")}, {entries, term + 1}}
  end

  defp batch_op(_kind, entries, term, _size, _last) do
    {{:hard_state, hard_state(term, "m1")}, {entries, term}}
  end

  defp violated?(%{violations: violations}), do: violations != []

  def put(process, {from, to}, value, revision, key \\ "k") do
    %Op{
      process: process,
      kind: :put,
      key: key,
      invoked_at: from,
      completed_at: to,
      value: value,
      result: {:ok, revision},
      status: :ok
    }
  end

  def get(process, {from, to}, result, key \\ "k") do
    %Op{
      process: process,
      kind: :get,
      key: key,
      invoked_at: from,
      completed_at: to,
      result: {:ok, result},
      status: :ok
    }
  end

  def compare_and_set(process, {from, to}, expected, value, result, key \\ "k") do
    %Op{
      process: process,
      kind: :compare_and_set,
      key: key,
      invoked_at: from,
      completed_at: to,
      expected: expected,
      value: value,
      result: result,
      status: :ok
    }
  end

  def delete(process, {from, to}, revision, key \\ "k") do
    %Op{
      process: process,
      kind: :delete,
      key: key,
      invoked_at: from,
      completed_at: to,
      result: {:ok, revision},
      status: :ok
    }
  end

  def unknown(%Op{} = op), do: %Op{op | status: :info, result: {:error, :timeout}}

  def failed(%Op{} = op), do: %Op{op | status: :fail, result: {:error, :not_leader}}

  def random_history(count, processes, seed) do
    rand = :rand.seed_s(:exsss, {seed, 11, 13})
    start = {nil, 0, rand, []}

    {_state, _rev, _rand, ops} = Enum.reduce(1..count, start, &random_request(&1, &2, processes))

    Enum.reverse(ops)
  end

  defp random_request(number, {value, revision, rand, acc}, processes) do
    {kind, after_kind} = :rand.uniform_s(2, rand)
    {before, after_before} = :rand.uniform_s(30, after_kind)
    {later, next} = :rand.uniform_s(30, after_before)
    instant = number * 10
    span = {instant - before + 1, instant + later}
    process = rem(number, processes) + 1

    if kind == 1 do
      fresh = "v#{number}"

      {fresh, number, next, [put(process, span, fresh, number) | acc]}
    else
      seen = if value == nil, do: nil, else: {value, revision}

      {value, revision, next, [get(process, span, seen) | acc]}
    end
  end

  def event_kinds(results) do
    for(result <- results, {kind, _time, _op} <- result.history, do: kind)
    |> Enum.uniq()
    |> Enum.sort()
  end

  def violation_kinds(result), do: result.violations |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

  def checked_violations(results) do
    for result <- results, result.violations != [], do: {result.config.seed, result.violations}
  end

  def unmatched_invokes(history) do
    for {:invoke, _time, op} <- history, not ended?(history, op), do: op
  end

  defp ended?(history, op) do
    Enum.any?(history, fn
      {:invoke, _time, _other} -> false
      {_status, _time, other} -> other.process == op.process and other.invoked_at == op.invoked_at
    end)
  end

  def ready_client(slot, session), do: %Client{slot: slot, session: session}

  def pending_client(choice) do
    client = ready_client(1, 7)

    Client.step(client, 100, choice)
  end
end
