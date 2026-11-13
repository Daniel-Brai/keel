defmodule Bench.Report do
  alias Bench.Stats

  @gib 1_073_741_824

  @notes """

  ## Notes and gaps

  - The read target is for a quiet cluster. Reads with 16 writers running are in the raw numbers.
  - The disk's power-loss protection is not known. One 4 KiB write with a datasync took about the p50 in the first row of the raw numbers. Every write pays at least that.
  - The 64 clients share one writer process on the benchmark node. The three logs share one disk.
  - Not measured: three separate hosts, other stores, a long soak run, open-loop load and a batching sweep.
  """

  @spec write(Path.t(), [{String.t(), String.t()}], map()) :: :ok
  def write(path, machine, results) do
    text = [
      "# Benchmark results\n\n",
      "Run on #{Date.utc_today()} with `make bench`. Every number below comes from this machine.\n\n",
      "## Machine\n\n",
      machine_table(machine),
      "\n## Against the targets\n\n",
      verdict_table(results),
      "\n## Method\n\n",
      method(results),
      "\n## Raw numbers\n\n",
      raw(results),
      @notes
    ]

    File.write!(path, text)
  end

  defp machine_table(machine) do
    rows = Enum.map(machine, fn {name, value} -> "| #{name} | #{value} |\n" end)

    ["| Item | Recorded |\n|---|---|\n" | rows]
  end

  defp verdict_table(results) do
    rows = [
      nfr3(results.failover),
      nfr4(results.write),
      nfr5(results.throughput),
      nfr6(results.read),
      nfr7(results.restart)
    ]

    ["| Target | Measured | Needed | Verdict |\n|---|---|---|---|\n" | Enum.map(rows, &row/1)]
  end

  defp row({name, measured, needed, met?}),
    do: "| #{name} | #{measured} | #{needed} | #{verdict(met?)} |\n"

  defp verdict(true), do: "met"
  defp verdict(false), do: "missed"

  defp nfr3(failover) do
    p99 = failover.p99 / 1_000

    measured =
      "p50 #{Stats.ms(failover.p50)} ms, p99 #{Stats.ms(failover.p99)} ms, max #{Stats.ms(failover.max)} ms over #{failover.count} kills"

    {"Failover", measured, "p99 under 1500 ms", p99 < 1_500}
  end

  defp nfr4(write) do
    measured = "p50 #{Stats.ms(write.p50)} ms, p99 #{Stats.ms(write.p99)} ms"

    {"Write latency, one client", measured, "p50 under 3 ms, p99 under 15 ms",
     write.p50 < 3_000 and write.p99 < 15_000}
  end

  defp nfr5(throughput) do
    measured = "#{throughput.per_second} writes/s, #{throughput.errors} errors"

    {"Write throughput, 64 clients", measured, "at least 10000 writes/s",
     throughput.per_second >= 10_000}
  end

  defp nfr6(read) do
    measured = "p50 #{Stats.ms(read.p50)} ms, p99 #{Stats.ms(read.p99)} ms"

    {"ReadIndex read latency", measured, "p99 under 5 ms", read.p99 < 5_000}
  end

  defp nfr7(restart) do
    seconds = restart.start_ms / 1_000
    scaled = seconds * @gib / restart.log_bytes

    measured =
      "#{:erlang.float_to_binary(seconds, decimals: 1)} s to recover #{mib(restart.log_bytes)} MiB, " <>
        "#{:erlang.float_to_binary(scaled, decimals: 1)} s scaled to 1 GiB"

    {"Restart", measured, "ready to vote within 10 s at 1 GiB of log (scaled and without a snapshot)",
     scaled <= 10}
  end

  defp mib(bytes), do: div(bytes, 1_048_576)

  defp method(results) do
    [
      "Three members on this host. Each member is a `:peer` BEAM node with its own data directory on the disk above. So the three logs share one device. ",
      "Distribution runs over loopback. Timers are the defaults. Ticks are 50 ms. The election timeout is 10 to 20 ticks. The heartbeat is every 2 ticks. Pre-vote is on.\n\n",
      "- Clients call the `Keel.KV` facade from the benchmark node. One writer process on that node serves every client. So 64 clients share one process.\n",
      "- One writer process holds one session. A session lets 16 commands wait at once by default. With 64 clients most writes then fail with `:too_many_pending`. So the run first raises `session_max_pending` to 128 with `set_limits`.\n",
      "- After the 64 client run the slowest follower needed #{results.catch_up_ms} ms to hold everything the leader held.\n",
      "- Values are 1 KiB of random bytes. The file system can't compress them.\n",
      "- Write latency: one client, closed loop, 500 warm-up writes dropped, #{results.write.count} measured.\n",
      "- Throughput: 64 clients, closed loop, 3 s of warm-up dropped, #{results.throughput.seconds} s measured.\n",
      "- Reads: `Keel.KV.get` with the default read index mode, one client, #{results.read.count} measured.\n",
      "- Failover: the leader is killed with SIGKILL. A write starts every 10 ms until one returns `:ok`. The time runs from the kill to that reply. Then the killed member restarts and the cluster settles before the next kill.\n",
      "- Restart: a follower holding about #{mib(results.restart.log_bytes)} MiB of segment files and no snapshot is killed and started again. The time is the call that opens its log and starts the group. It leaves out the #{results.restart.boot_ms} ms to boot the VM. ",
      "The member then needed #{results.restart.caught_up_ms} ms to bring its state machine level with the leader. The 1 GiB figure is the measured time scaled by size. A log with a 256 MiB snapshot on top was not measured.\n",
      "- Segment files are preallocated. Their size is an upper bound on the log bytes. The log held #{results.restart.entries} entries.\n"
    ]
  end

  defp raw(results) do
    [
      "| Run | Samples | p50 ms | p90 ms | p99 ms | max ms |\n|---|---|---|---|---|---|\n",
      raw_row("4 KiB write plus datasync", results.baseline),
      raw_row("Write, one client", results.write),
      raw_row("Write, 64 clients", results.throughput.latency),
      raw_row("Read, one client", results.read),
      raw_row("Read, 16 writers running", results.read_loaded),
      raw_row("Failover", results.failover)
    ]
  end

  defp raw_row(name, summary) do
    cells = [summary.count, ms(summary.p50), ms(summary.p90), ms(summary.p99), ms(summary.max)]

    "| #{name} | #{Enum.join(cells, " | ")} |\n"
  end

  defp ms(micros), do: Stats.ms(micros)
end
