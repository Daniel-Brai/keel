defmodule Bench.Workloads do
  alias Bench.Cluster
  alias Bench.Stats
  alias Keel.KV

  @value_bytes 1_024

  @doc "One 4 KiB append and one datasync, again and again, on the data disk."
  @spec sync_baseline(Path.t(), pos_integer()) :: Stats.summary()
  def sync_baseline(dir, count) do
    File.mkdir_p!(dir)
    path = Path.join(dir, "sync-baseline.bin")
    {:ok, file} = :file.open(path, [:raw, :binary, :append])
    block = :crypto.strong_rand_bytes(4_096)

    samples = Enum.map(1..count, fn _number -> timed(fn -> synced_write(file, block) end) end)

    :ok = :file.close(file)
    File.rm!(path)

    Stats.summary(samples)
  end

  @doc "One client in a closed loop with 1 KiB values."
  @spec write_latency(Cluster.t(), pos_integer(), pos_integer()) :: Stats.summary()
  def write_latency(cluster, warmup, count) do
    Enum.each(1..warmup, &put!(cluster, "warm-#{&1}"))

    samples = Enum.map(1..count, fn number -> timed(fn -> put!(cluster, "lat-#{number}") end) end)

    Stats.summary(samples)
  end

  @doc "Linearizable reads by one client in a closed loop."
  @spec read_latency(Cluster.t(), pos_integer(), pos_integer()) :: Stats.summary()
  def read_latency(cluster, warmup, count) do
    put!(cluster, "read-key")
    Enum.each(1..warmup, fn _number -> get!(cluster) end)

    samples = Enum.map(1..count, fn _number -> timed(fn -> get!(cluster) end) end)

    Stats.summary(samples)
  end

  @doc "Reads while `writers` clients keep writing."
  @spec read_latency_under_load(Cluster.t(), pos_integer(), pos_integer()) :: Stats.summary()
  def read_latency_under_load(cluster, writers, count) do
    stop = :atomics.new(1, [])
    tasks = for id <- 1..writers, do: Task.async(fn -> write_until(cluster, "rw-#{id}", stop) end)

    summary = read_latency(cluster, 200, count)

    :atomics.put(stop, 1, 1)
    Enum.each(tasks, &Task.await(&1, 60_000))

    summary
  end

  @doc "Closed loop with `clients` writers. The first seconds are discarded."
  @spec throughput(Cluster.t(), pos_integer(), pos_integer(), pos_integer()) :: map()
  def throughput(cluster, clients, warmup_s, measure_s) do
    now = System.monotonic_time(:millisecond)
    from = now + warmup_s * 1_000
    until = from + measure_s * 1_000
    tasks = for id <- 1..clients, do: Task.async(fn -> client_loop(cluster, id, from, until) end)

    results = Enum.map(tasks, &Task.await(&1, (warmup_s + measure_s) * 1_000 + 120_000))
    samples = Enum.flat_map(results, & &1.samples)
    errors = results |> Enum.map(& &1.errors) |> Enum.sum()
    done = length(samples)

    %{
      clients: clients,
      seconds: measure_s,
      writes: done,
      errors: errors,
      per_second: div(done, measure_s),
      latency: Stats.summary(samples)
    }
  end

  @doc "Milliseconds until every member holds what the leader holds."
  @spec settle(Cluster.t()) :: non_neg_integer()
  def settle(cluster) do
    {micros, :ok} = :timer.tc(fn -> Cluster.wait!(cluster, &Cluster.converged?/1, 900_000) end)

    div(micros, 1_000)
  end

  @doc "Kills the leader `kills` times and times the wait for the next committed write."
  @spec failover(Cluster.t(), pos_integer()) :: {Cluster.t(), Stats.summary()}
  def failover(cluster, kills) do
    {final, samples} =
      Enum.reduce(1..kills, {cluster, []}, fn _round, {current, found} ->
        {next, micros} = kill_and_wait(current)

        {next, [micros | found]}
      end)

    {final, Stats.summary(samples)}
  end

  @doc "Writes until a follower holds `megabytes` of segments. Returns the follower."
  @spec fill(Cluster.t(), pos_integer()) :: String.t()
  def fill(cluster, megabytes) do
    statuses = Cluster.status(cluster)
    leader = Cluster.leader_of(statuses)
    follower = statuses |> Map.keys() |> Enum.sort() |> Enum.find(&(&1 != leader))

    fill_until(cluster, follower, megabytes * 1_048_576)

    follower
  end

  @doc "Kills a follower and starts it again. Times the log recovery and the catch up."
  @spec restart(Cluster.t(), String.t()) :: map()
  def restart(cluster, id) do
    :ok = Cluster.wait!(cluster, &level?(&1, id), 900_000)

    bytes = Cluster.log_bytes(cluster, id)
    before = Cluster.status(cluster)
    {:ok, status} = Map.fetch!(before, id)
    dead = Cluster.kill!(cluster, id)
    {restarted, times} = Cluster.restart!(dead, id)
    started = System.monotonic_time(:millisecond)

    :ok = Cluster.wait!(restarted, &level?(&1, id), 900_000)

    ready_ms = System.monotonic_time(:millisecond) - started

    Map.merge(times, %{
      log_bytes: bytes,
      entries: status.last_index,
      caught_up_ms: ready_ms,
      cluster: restarted
    })
  end

  # The member holds what the leader holds. Another follower may still be behind
  defp level?(statuses, id) do
    leader = Cluster.leader_of(statuses)

    case {leader, Map.fetch!(statuses, id)} do
      {nil, _member} -> false
      {leader, {:ok, member}} -> level_with?(Map.fetch!(statuses, leader), member)
      {_leader, _down} -> false
    end
  end

  defp level_with?({:ok, head}, member), do: member.applied >= head.commit
  defp level_with?(_down, _member), do: false

  defp kill_and_wait(cluster) do
    :ok = Cluster.wait!(cluster, &Cluster.converged?/1, 300_000)

    statuses = Cluster.status(cluster)
    leader = Cluster.leader_of(statuses)
    started = System.monotonic_time(:microsecond)
    dead = Cluster.kill!(cluster, leader)
    micros = probe_until_written(dead, started)
    {restarted, _times} = Cluster.restart!(dead, leader)

    {restarted, micros}
  end

  # An open loop. A new write starts every 10 ms whether or not the others came back
  defp probe_until_written(cluster, started) do
    parent = self()
    ref = make_ref()

    probe = fn ->
      case KV.put(cluster.group, "probe", "v", timeout: 10_000) do
        {:ok, _revision} -> send(parent, {ref, System.monotonic_time(:microsecond)})
        _failed -> :ok
      end
    end

    micros = probe_loop(probe, ref, started)
    flush(ref)

    micros
  end

  defp probe_loop(probe, ref, started) do
    spawn(probe)

    receive do
      {^ref, finished} -> finished - started
    after
      10 -> probe_loop(probe, ref, started)
    end
  end

  defp flush(ref) do
    receive do
      {^ref, _finished} -> flush(ref)
    after
      200 -> :ok
    end
  end

  defp fill_until(cluster, follower, target) do
    bytes = Cluster.log_bytes(cluster, follower)

    case bytes >= target do
      true ->
        :ok

      false ->
        _batch = throughput(cluster, 64, 0, 5)

        fill_until(cluster, follower, target)
    end
  end

  defp client_loop(cluster, id, from, until) do
    loop = %{cluster: cluster, id: id, from: from, until: until}

    client_step(loop, 0, [], 0)
  end

  defp client_step(loop, number, samples, errors) do
    started = System.monotonic_time(:millisecond)

    case started >= loop.until do
      true -> %{samples: samples, errors: errors}
      false -> client_write(loop, number, samples, errors, started)
    end
  end

  defp client_write(loop, number, samples, errors, started) do
    begin = System.monotonic_time(:microsecond)
    result = KV.put(loop.cluster.group, "tp-#{loop.id}-#{number}", value(), timeout: 15_000)
    finish = System.monotonic_time(:microsecond)

    case {result, started >= loop.from} do
      {{:ok, _revision}, true} ->
        client_step(loop, number + 1, [finish - begin | samples], errors)

      {{:ok, _revision}, false} ->
        client_step(loop, number + 1, samples, errors)

      {_failed, _measuring} ->
        client_step(loop, number + 1, samples, errors + 1)
    end
  end

  defp write_until(cluster, prefix, stop) do
    case :atomics.get(stop, 1) do
      1 ->
        :ok

      _running ->
        KV.put(cluster.group, "#{prefix}-#{System.unique_integer([:positive])}", value())

        write_until(cluster, prefix, stop)
    end
  end

  defp synced_write(file, block) do
    :ok = :file.write(file, block)
    :ok = :file.datasync(file)
  end

  defp put!(cluster, key) do
    {:ok, _revision} = KV.put(cluster.group, key, value(), timeout: 15_000)

    :ok
  end

  defp get!(cluster) do
    {:ok, %{}} = KV.get(cluster.group, "read-key", timeout: 15_000)

    :ok
  end

  defp value, do: :crypto.strong_rand_bytes(@value_bytes)

  defp timed(fun) do
    started = System.monotonic_time(:microsecond)

    fun.()

    System.monotonic_time(:microsecond) - started
  end
end
