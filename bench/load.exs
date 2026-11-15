# Run with `make bench.load`. It needs a free disk.
# KEEL_LOAD_PARTS picks parts from open, sweep and soak. KEEL_SOAK_MINUTES sets the soak length.
# KEEL_SOAK_SNAPSHOT_BYTES makes snapshots come sooner.

Enum.each(~w[stats machine cluster workloads], fn name ->
  Code.require_file("#{name}.exs", __DIR__)
end)

defmodule Bench.Load do
  alias Bench.Cluster
  alias Bench.Machine
  alias Bench.Stats
  alias Bench.Workloads
  alias Keel.KV

  @value_bytes 1_024
  @soak_keys 10_000
  @tick_ms 1

  @type open_run :: %{
          rate: pos_integer(),
          achieved: non_neg_integer(),
          errors: non_neg_integer(),
          shed: non_neg_integer(),
          reasons: %{term() => pos_integer()},
          latency: Stats.summary()
        }

  @type sweep_run :: %{
          label: String.t(),
          per_second: non_neg_integer(),
          errors: non_neg_integer(),
          latency: Stats.summary()
        }

  @type soak_minute :: %{
          minute: pos_integer(),
          achieved: non_neg_integer(),
          errors: non_neg_integer(),
          reasons: %{term() => pos_integer()},
          latency: Stats.summary(),
          memory_mib: [non_neg_integer()],
          disk_mib: non_neg_integer()
        }

  def run do
    # Each run gets its own folder so two runs can never delete each other's data
    run_id = System.unique_integer([:positive])
    data = Path.expand("data-load-#{run_id}", __DIR__)
    parts = parts()

    File.rm_rf!(data)
    File.mkdir_p!(data)

    machine = Machine.describe(data)
    path = Path.expand("load.md", __DIR__)

    # The report is written after each part so a crash keeps what was measured
    open = run_part(parts, "open", fn -> open_loop(Path.join(data, "open")) end)
    File.write!(path, report(machine, open, [], []))

    sweep = run_part(parts, "sweep", fn -> batching_sweep(Path.join(data, "sweep")) end)
    File.write!(path, report(machine, open, sweep, []))

    written = fn rows -> File.write!(path, report(machine, open, sweep, rows)) end
    run_part(parts, "soak", fn -> soak(Path.join(data, "soak"), written) end)

    File.rm_rf!(data)
    say("wrote #{path}")
  end

  @doc "Writes arrive at fixed rates whether or not earlier ones finished. Latency runs from the planned start."
  @spec open_loop(Path.t()) :: [open_run()]
  def open_loop(dir) do
    rates = env_list("KEEL_OPEN_RATES", [500, 1_000, 2_000, 3_000, 4_000, 5_000])
    seconds = env("KEEL_OPEN_SECONDS", 10)
    cluster = start_open!(dir)

    runs =
      Enum.map(rates, fn rate ->
        say("open loop at #{rate} writes/s")
        run = offered(cluster, rate, seconds, "open-#{rate}")
        Workloads.settle(cluster)

        run
      end)

    Cluster.stop(cluster)

    runs
  end

  @doc "Closed loop throughput for each batch size and window and for each client count."
  @spec batching_sweep(Path.t()) :: [sweep_run()]
  def batching_sweep(dir) do
    seconds = env("KEEL_SWEEP_SECONDS", 8)

    batches =
      for entries <- [16, 64, 256], window <- [1, 8, 32] do
        {"max_append_entries #{entries}, max_inflight_appends #{window}, 64 clients",
         [max_append_entries: entries, max_inflight_appends: window], 64}
      end

    clients = for count <- [8, 32, 128], do: {"defaults, #{count} clients", [], count}

    Enum.with_index(batches ++ clients, fn {label, raft, count}, index ->
      say("sweep #{label}")
      cluster = Cluster.start!(Path.join(dir, Integer.to_string(index)), raft)
      result = Workloads.throughput(cluster, count, 2, seconds)
      Cluster.stop(cluster)

      %{
        label: label,
        per_second: result.per_second,
        errors: result.errors,
        latency: result.latency
      }
    end)
  end

  @doc "One fixed rate for many minutes on a bounded key space. Each minute gets its own row."
  @spec soak(Path.t(), ([soak_minute()] -> :ok)) :: [soak_minute()]
  def soak(dir, written) do
    minutes = env("KEEL_SOAK_MINUTES", 60)
    rate = env("KEEL_SOAK_RATE", 1_500)
    snapshot_bytes = env("KEEL_SOAK_SNAPSHOT_BYTES", 268_435_456)
    cluster = start_open!(dir, snapshot_threshold_bytes: snapshot_bytes)

    rows =
      Enum.reduce(1..minutes, [], fn minute, done ->
        run = offered(cluster, rate, 60, "soak")
        memory = Enum.map(Cluster.nodes(cluster), &memory_mib/1)
        disk = disk_mib(dir)

        say(
          "soak minute #{minute}: #{run.achieved}/s, p99 #{Stats.ms(run.latency.p99)} ms, errors #{inspect(run.reasons)}, memory #{inspect(memory, charlists: :as_lists)} MiB"
        )

        stalled(cluster, run, rate)

        row = %{
          minute: minute,
          achieved: run.achieved,
          errors: run.errors + run.shed,
          reasons: run.reasons,
          latency: run.latency,
          memory_mib: memory,
          disk_mib: disk
        }

        rows = Enum.concat(done, [row])
        written.(rows)

        rows
      end)

    Cluster.stop(cluster)

    rows
  end

  # Open loop runs past the 128 pending that the closed loop runs use
  defp start_open!(dir, raft \\ []) do
    cluster = Cluster.start!(dir, raft)
    limits = %{session_max_pending: 1_000}
    {:ok, _result} = KV.Writer.submit(cluster.group, {:set_limits, limits}, timeout: 15_000)

    cluster
  end

  # A planned write that finds more than this many still running is shed and counted
  @max_running 4_096

  defp offered(cluster, rate, seconds, prefix) do
    interval_us = div(1_000_000, rate)
    total = rate * seconds
    started = System.monotonic_time(:microsecond)

    loop = %{
      cluster: cluster,
      prefix: prefix,
      interval_us: interval_us,
      total: total,
      started: started
    }

    state = dispatch(loop, %{sent: 0, running: 0, shed: 0, samples: [], errors: 0, reasons: %{}})
    finished = drain(state)
    elapsed_s = (System.monotonic_time(:microsecond) - started) / 1_000_000
    done = length(finished.samples)

    %{
      rate: rate,
      achieved: round(done / elapsed_s),
      errors: finished.errors,
      shed: finished.shed,
      reasons: finished.reasons,
      latency: Stats.summary(finished.samples)
    }
  end

  defp dispatch(%{total: total}, %{sent: total} = state), do: state

  defp dispatch(loop, state) do
    now = System.monotonic_time(:microsecond)
    due = min(div(now - loop.started, loop.interval_us) + 1, loop.total)
    launched = launch(loop, state, due)
    collected = collect(launched, @tick_ms)

    dispatch(loop, collected)
  end

  defp launch(_loop, %{sent: sent} = state, due) when sent >= due, do: state

  defp launch(loop, %{running: running} = state, due) when running >= @max_running do
    launch(loop, %{state | sent: state.sent + 1, shed: state.shed + 1}, due)
  end

  defp launch(loop, state, due) do
    planned = loop.started + state.sent * loop.interval_us
    key = key(loop.prefix, state.sent)
    parent = self()

    spawn_link(fn -> send(parent, {:written, planned, put(loop.cluster, key)}) end)

    launch(loop, %{state | sent: state.sent + 1, running: state.running + 1}, due)
  end

  defp collect(state, wait_ms) do
    receive do
      {:written, planned, result} ->
        finished = System.monotonic_time(:microsecond)
        counted = count(state, result, finished - planned)

        collect(counted, 0)
    after
      wait_ms -> state
    end
  end

  defp drain(%{running: 0} = state), do: state

  defp drain(state) do
    collected = collect(state, 100)

    drain(collected)
  end

  defp count(state, {:ok, _revision}, latency),
    do: %{state | running: state.running - 1, samples: [latency | state.samples]}

  defp count(state, failed, _latency) do
    reason = reason(failed)
    reasons = Map.update(state.reasons, reason, 1, &(&1 + 1))

    %{state | running: state.running - 1, errors: state.errors + 1, reasons: reasons}
  end

  defp reason({:error, {kind, _detail}}), do: kind
  defp reason({:error, kind}), do: kind
  defp reason(other), do: other

  defp put(cluster, key) do
    value = :crypto.strong_rand_bytes(@value_bytes)

    KV.put(cluster.group, key, value, timeout: 15_000)
  end

  defp key("soak", number), do: "soak-#{rem(number, @soak_keys)}"
  defp key(prefix, number), do: "#{prefix}-#{number}"

  # A minute that barely moved prints what each member is doing so a stall can be traced
  defp stalled(cluster, run, rate) when run.achieved * 10 < rate do
    statuses = Cluster.status(cluster)

    Enum.each(statuses, fn {id, status} -> say("stalled #{id}: #{inspect(status, limit: 20)}") end)

    Enum.each(Cluster.nodes(cluster), &busy_mailboxes/1)
  end

  defp stalled(_cluster, _run, _rate), do: :ok

  defp busy_mailboxes(node) do
    names = :erpc.call(node, :erlang, :registered, [], 5_000)

    Enum.each(names, fn name ->
      pid = :erpc.call(node, :erlang, :whereis, [name], 5_000)

      info =
        :erpc.call(
          node,
          :erlang,
          :process_info,
          [pid, [:message_queue_len, :current_function]],
          5_000
        )

      case info do
        [message_queue_len: queued, current_function: function] when queued > 1_000 ->
          say("stalled #{node} #{name}: #{queued} queued in #{inspect(function)}")

        _quiet ->
          :ok
      end
    end)
  end

  defp memory_mib(node) do
    bytes = :erpc.call(node, :erlang, :memory, [:total], 5_000)

    div(bytes, 1_048_576)
  end

  defp disk_mib(dir) do
    {text, 0} = System.cmd("du", ["-sm", dir])
    [mib | _rest] = String.split(text)

    String.to_integer(mib)
  end

  defp run_part(parts, name, fun) do
    case name in parts do
      true -> fun.()
      false -> []
    end
  end

  defp parts do
    raw = System.get_env("KEEL_LOAD_PARTS", "open,sweep,soak")

    String.split(raw, ",", trim: true)
  end

  defp env(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp env_list(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> value |> String.split(",", trim: true) |> Enum.map(&String.to_integer/1)
    end
  end

  defp report(machine, open, sweep, soak) do
    [
      "# Load results\n\n",
      "Run on #{Date.utc_today()} with `make bench.load`. Three members on this host with 1 KiB values.\n\n",
      "## Machine\n\n",
      "| Item | Recorded |\n|---|---|\n",
      Enum.map(machine, fn {name, value} -> "| #{name} | #{value} |\n" end),
      open_section(open),
      sweep_section(sweep),
      soak_section(soak)
    ]
  end

  defp open_section([]), do: []

  defp open_section(runs) do
    [
      "\n## Open loop\n\n",
      "Writes start on a fixed schedule whether or not earlier ones finished. Latency runs from the planned start. So a backlog shows up as latency. A write that would be the #{@max_running + 1}th still running is shed.\n\n",
      "| Offered/s | Achieved/s | p50 ms | p99 ms | max ms | Errors | Shed |\n|---|---|---|---|---|---|---|\n",
      Enum.map(runs, fn run ->
        "| #{run.rate} | #{run.achieved} | #{Stats.ms(run.latency.p50)} | #{Stats.ms(run.latency.p99)} | #{Stats.ms(run.latency.max)} | #{run.errors} | #{run.shed} |\n"
      end)
    ]
  end

  defp sweep_section([]), do: []

  defp sweep_section(runs) do
    [
      "\n## Batching sweep\n\n",
      "Closed loop. Each row is a fresh cluster. Two seconds of warm-up are dropped.\n\n",
      "| Setting | Writes/s | p50 ms | p99 ms | Errors |\n|---|---|---|---|---|\n",
      Enum.map(runs, fn run ->
        "| #{run.label} | #{run.per_second} | #{Stats.ms(run.latency.p50)} | #{Stats.ms(run.latency.p99)} | #{run.errors} |\n"
      end)
    ]
  end

  defp soak_section([]), do: []

  defp soak_section(rows) do
    [
      "\n## Soak\n\n",
      "One cluster at a fixed open-loop rate on #{@soak_keys} keys. So the stored state stays the same size and only the log grows. Memory is each member's BEAM total. Disk is all three data directories.\n\n",
      "| Minute | Achieved/s | p50 ms | p99 ms | max ms | Errors | Memory MiB | Disk MiB |\n|---|---|---|---|---|---|---|---|\n",
      Enum.map(rows, fn row ->
        "| #{row.minute} | #{row.achieved} | #{Stats.ms(row.latency.p50)} | #{Stats.ms(row.latency.p99)} | #{Stats.ms(row.latency.max)} | #{errors_cell(row)} | #{Enum.join(row.memory_mib, " / ")} | #{row.disk_mib} |\n"
      end)
    ]
  end

  defp errors_cell(%{errors: 0}), do: "0"
  defp errors_cell(row), do: "#{row.errors} #{inspect(row.reasons)}"

  defp say(text), do: IO.puts("[load] #{text}")
end

Bench.Load.run()
