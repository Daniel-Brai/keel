# Run with `make bench`. It needs a free disk and takes about fifteen minutes.

Enum.each(~w[stats machine cluster workloads report], fn name ->
  Code.require_file("#{name}.exs", __DIR__)
end)

defmodule Bench.Main do
  alias Bench.Cluster
  alias Bench.Machine
  alias Bench.Report
  alias Bench.Workloads

  @snapshot_never 1_099_511_627_776

  def run do
    data = Path.expand("data", __DIR__)
    kills = env("KEEL_BENCH_KILLS", 100)
    log_mb = env("KEEL_BENCH_LOG_MB", 300)
    seconds = env("KEEL_BENCH_SECONDS", 15)

    File.rm_rf!(data)
    File.mkdir_p!(data)

    say("sync baseline")
    baseline = Workloads.sync_baseline(data, 2_000)

    say("cluster with default timers")
    cluster = Cluster.start!(Path.join(data, "default"), [])

    say("write latency")
    write = Workloads.write_latency(cluster, 500, 5_000)

    say("read latency")
    read = Workloads.read_latency(cluster, 200, 5_000)
    read_loaded = Workloads.read_latency_under_load(cluster, 16, 3_000)

    say("throughput with 64 clients")
    throughput = Workloads.throughput(cluster, 64, 3, seconds)

    say("waiting for the followers to catch up")
    catch_up_ms = Workloads.settle(cluster)

    say("failover over #{kills} kills")
    {after_kills, failover} = Workloads.failover(cluster, kills)

    Cluster.stop(after_kills)
    File.rm_rf!(Path.join(data, "default"))

    say("cluster for the restart run, #{log_mb} MiB of log")
    restart = restart_run(Path.join(data, "restart"), log_mb)

    results = %{
      baseline: baseline,
      write: write,
      read: read,
      read_loaded: read_loaded,
      throughput: throughput,
      catch_up_ms: catch_up_ms,
      failover: failover,
      restart: restart
    }

    machine = Machine.describe(data)
    path = Path.expand("results.md", __DIR__)

    Report.write(path, machine, results)
    File.rm_rf!(data)
    say("wrote #{path}")
  end

  defp restart_run(dir, log_mb) do
    cluster = Cluster.start!(dir, snapshot_threshold_bytes: @snapshot_never)
    follower = Workloads.fill(cluster, log_mb)
    restart = Workloads.restart(cluster, follower)

    Cluster.stop(restart.cluster)

    Map.delete(restart, :cluster)
  end

  defp env(name, default) do
    case System.get_env(name) do
      nil -> default
      text -> String.to_integer(text)
    end
  end

  defp say(text), do: IO.puts("[bench] #{text}")
end

Bench.Main.run()
