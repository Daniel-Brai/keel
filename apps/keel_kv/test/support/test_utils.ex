defmodule Keel.KV.TestUtils do
  use ExUnitProperties

  import ExUnit.Callbacks

  alias Keel.Common.Name
  alias Keel.Env.Disk.Stub
  alias Keel.KV
  alias Keel.KV.Command
  alias Keel.KV.Limits
  alias Keel.KV.Machine
  alias Keel.KV.Result
  alias Keel.KV.State
  alias Keel.KV.TestCluster
  alias Keel.KV.Writer
  alias Keel.Raft.Entry

  def machine(config \\ %{}), do: Machine.init(config)

  def entry(index, time_ms, data),
    do: %Entry{term: 1, index: index, kind: :command, data: data, time_ms: time_ms}

  def run(state, {index, time_ms}, {client_id, seq, ack}, op) do
    command = %Command{client_id: client_id, seq: seq, ack: ack, op: op}
    data = Command.encode(command)
    entry = entry(index, time_ms, data)

    Machine.apply(state, entry)
  end

  def register(state, index, time_ms) do
    {next, {:ok, client_id}} = run(state, {index, time_ms}, {0, 0, 0}, :register)

    {next, client_id}
  end

  def populated(count, keys, config \\ %{}) do
    {registered, client_id} = register(machine(config), 1, 0)
    indexed = keys |> Enum.take(count) |> Enum.with_index(2)

    store =
      Enum.reduce(indexed, registered, fn {key, index}, state ->
        {next, {:ok, ^index}} =
          run(state, {index, index}, {client_id, index, index}, {:put, key, key})

        next
      end)

    {store, client_id}
  end

  def sessions(state, count, first_index) do
    last = first_index + count - 1

    Enum.reduce(first_index..last, state, fn index, acc ->
      {next, _client_id} = register(acc, index, index)
      next
    end)
  end

  def keys(count),
    do: Enum.map(1..count, &"key-#{String.pad_leading(Integer.to_string(&1), 4, "0")}")

  def session_ids(%State{sessions: sessions}), do: :gb_trees.keys(sessions)

  def undetected_flips(snapshot) do
    bits = byte_size(snapshot) * 8

    for position <- 0..(bits - 1), flipped?(snapshot, position), do: position
  end

  defp flipped?(snapshot, position) do
    byte = div(position, 8)
    <<before::binary-size(^byte), value, rest::binary>> = snapshot
    changed = Bitwise.bxor(value, Bitwise.bsl(1, rem(position, 8)))
    corrupt = <<before::binary, changed, rest::binary>>

    Machine.restore(corrupt) != {:error, :corrupt_snapshot}
  end

  def ops do
    [
      :register,
      {:put, "key", "value"},
      {:put, "key", ""},
      {:delete, "key"},
      {:compare_and_set, "key", 0, {:put, "value"}},
      {:compare_and_set, "key", 7, :delete},
      {:set_limits, %{max_key_bytes: 9, session_ttl_ms: 77}},
      {:get, "key"},
      {:range, "a", nil, 5},
      {:range, "a", "z", 5},
      {:prefix, "ab", nil, 5},
      {:prefix, "ab", "abc", 5}
    ]
  end

  def roundtrip_failures(ops) do
    Enum.reject(ops, fn op ->
      command = %Command{client_id: 3, seq: 4, ack: 2, op: op}
      bytes = Command.encode(command)

      Command.decode(bytes) == {:ok, command}
    end)
  end

  def decoding_prefixes(bytes) do
    cuts = 0..(byte_size(bytes) - 1)

    Enum.filter(cuts, &decodes?(bytes, &1))
  end

  defp decodes?(bytes, length) do
    cut = binary_part(bytes, 0, length)

    match?({:ok, _command}, Command.decode(cut))
  end

  def restoring_prefixes(snapshot) do
    cuts = 0..(byte_size(snapshot) - 1)

    Enum.filter(cuts, &restores?(snapshot, &1))
  end

  defp restores?(snapshot, length) do
    cut = binary_part(snapshot, 0, length)

    match?({:ok, _state}, Machine.restore(cut))
  end

  def results do
    [
      {:ok, nil},
      {:ok, 0},
      {:ok, 12_345},
      {:ok, {"value", 9}},
      {:ok, {"", 9}},
      {:ok, {[], nil}},
      {:ok, {[{"a", "1", 2}, {"b", "", 3}], "c"}},
      {:error, :space_exceeded},
      {:error, :session_expired},
      {:error, {:revision_mismatch, 41}}
    ]
  end

  def result_failures(results) do
    Enum.reject(results, fn result ->
      bytes = Result.encode(result)

      Result.decode(bytes) == {:ok, result}
    end)
  end

  def model_mismatches(ops) do
    {registered, {:ok, client}} = apply_command(Machine.init(%{}), 1, {0, 0, 0}, :register)
    start = {registered, %{}, []}
    indexed = Enum.with_index(ops, 2)

    {state, model, found} =
      Enum.reduce(indexed, start, fn {op, index}, acc -> compare(acc, op, index, client) end)

    keys = ops |> Enum.map(&elem(&1, 1)) |> Enum.uniq()

    held = Enum.reject(keys, &(Machine.fetch(state, &1) == Map.get(model, &1)))

    Enum.reverse(found) ++ Enum.map(held, &{:final, &1})
  end

  defp compare({state, model, found}, op, index, client) do
    {next, result} = apply_command(state, index, {client, index, index}, op)
    {expected, kept} = expect(model, op, index)
    mismatches = if result == expected, do: found, else: [{index, op, result, expected} | found]

    {next, kept, mismatches}
  end

  def apply_command(state, index, {client_id, seq, ack}, op) do
    command = %Command{client_id: client_id, seq: seq, ack: ack, op: op}
    data = Command.encode(command)
    entry = %Entry{term: 1, index: index, kind: :command, data: data, time_ms: index}

    Machine.apply(state, entry)
  end

  defp expect(model, {:put, key, value}, index),
    do: {{:ok, index}, Map.put(model, key, {value, index})}

  defp expect(model, {:delete, key}, index) do
    if Map.has_key?(model, key),
      do: {{:ok, index}, Map.delete(model, key)},
      else: {{:ok, 0}, model}
  end

  defp expect(model, {:get, key}, _index) do
    case Map.fetch(model, key) do
      {:ok, record} -> {{:ok, record}, model}
      :error -> {{:ok, nil}, model}
    end
  end

  defp expect(model, {:compare_and_set, key, expected, change}, index) do
    current = model |> Map.get(key, {nil, 0}) |> elem(1)

    cond do
      current != expected -> {{:error, {:revision_mismatch, current}}, model}
      change == :delete and current == 0 -> {{:error, {:revision_mismatch, 0}}, model}
      change == :delete -> {{:ok, index}, Map.delete(model, key)}
      true -> {{:ok, index}, Map.put(model, key, {elem(change, 1), index})}
    end
  end

  def machine_after(ops) do
    {registered, {:ok, client}} = apply_command(Machine.init(%{}), 1, {0, 0, 0}, :register)
    {state, _results} = continue(registered, client, 2, ops)

    {state, client}
  end

  def continue(state, client, first, ops) do
    indexed = Enum.with_index(ops, first)

    {final, results} =
      Enum.reduce(indexed, {state, []}, fn {op, index}, {acc, found} ->
        {next, result} = apply_command(acc, index, {client, index, index}, op)

        {next, [result | found]}
      end)

    {final, Enum.reverse(results)}
  end

  def decode_class(bytes) do
    case Command.decode(bytes) do
      {:ok, %Command{}} -> :ok
      {:error, :malformed_command} -> :malformed
    end
  end

  def apply_bytes(state, bytes) do
    entry = %Entry{term: 1, index: 99, kind: :command, data: bytes, time_ms: 99}
    {_next, result} = Machine.apply(state, entry)

    elem(result, 0)
  end

  def random_key do
    StreamData.one_of([
      StreamData.member_of(~w[a b c d e]),
      StreamData.binary(min_length: 1, max_length: 6)
    ])
  end

  def random_write do
    value = StreamData.binary(max_length: 8)
    expected = StreamData.integer(0..40)

    StreamData.frequency([
      {4, gen(all(key <- random_key(), value <- value, do: {:put, key, value}))},
      {2, gen(all(key <- random_key(), do: {:delete, key}))},
      {3,
       gen(
         all(
           key <- random_key(),
           expected <- expected,
           value <- value,
           do: {:compare_and_set, key, expected, {:put, value}}
         )
       )},
      {1,
       gen(
         all(
           key <- random_key(),
           expected <- expected,
           do: {:compare_and_set, key, expected, :delete}
         )
       )},
      {3, gen(all(key <- random_key(), do: {:get, key}))}
    ])
  end

  def random_request do
    bound = StreamData.one_of([StreamData.constant(nil), StreamData.binary(max_length: 6)])
    limit = StreamData.integer(1..100)
    limit_names = Map.keys(Limits.defaults())
    names = StreamData.member_of(limit_names)
    pairs = StreamData.list_of({names, StreamData.integer(0..100_000)}, max_length: 8)
    changes = StreamData.map(pairs, &Map.new/1)

    StreamData.one_of([
      random_write(),
      StreamData.constant(:register),
      gen(all(changes <- changes, do: {:set_limits, changes})),
      gen(all(key <- random_key(), to <- bound, limit <- limit, do: {:range, key, to, limit})),
      gen(
        all(
          key <- random_key(),
          after_key <- bound,
          limit <- limit,
          do: {:prefix, key, after_key, limit}
        )
      )
    ])
  end

  def random_command do
    number = StreamData.integer(0..1_000_000)

    gen all(client <- number, seq <- number, ack <- number, op <- random_request()) do
      %Command{client_id: client, seq: seq, ack: ack, op: op}
    end
  end

  def start_relay!(label) do
    test = self()
    spec = Supervisor.child_spec({Task, fn -> relay(test, label) end}, id: label)

    start_supervised!(spec)
  end

  def start_writer!(relays, extra \\ []) do
    group = Name.build("w", :writer, Integer.to_string(System.unique_integer([:positive])))
    nodes = Keyword.keys(relays)
    route = fn node -> Keyword.fetch!(relays, node) end
    timing = [attempt_timeout_ms: 100, retry_backoff_ms: 5, retry_backoff_max_ms: 20]
    opts = Keyword.merge(timing, [group: group, nodes: nodes, route: route] ++ extra)

    start_supervised!({Writer, opts})

    group
  end

  def put_async!(group, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2_000)

    Task.async(fn -> Writer.submit(group, {:put, "key", "value"}, timeout: timeout) end)
  end

  def read_async!(group, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2_000)

    Task.async(fn -> Writer.read(group, {:get, "key"}, timeout: timeout) end)
  end

  def kill!(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    end
  end

  def reply(from, answer), do: GenServer.reply(from, answer)

  defp relay(test, label) do
    receive do
      {:"$gen_call", from, {:propose, bytes}} ->
        {:ok, command} = Command.decode(bytes)
        send(test, {label, from, command})
        relay(test, label)

      {:"$gen_call", from, {:read, query}} ->
        send(test, {label, from, {:read, query}})
        relay(test, label)
    end
  end

  # Power loss harness. It runs on a machine set up for it and skips elsewhere

  @lazyfs_wait_ms 15_000
  @device_bytes 1_073_741_824
  @log_bytes 4_294_967_296
  @power_tools ~w[dmsetup losetup mkfs.ext4 mount umount replay-log]

  def lazyfs_skip do
    binary = System.get_env("KEEL_LAZYFS", "lazyfs")
    found = System.find_executable(binary)
    fuse = System.find_executable("fusermount3")
    device? = File.exists?("/dev/fuse")

    cond do
      found == nil ->
        skipped(:lazyfs, "LazyFS is not installed. Put lazyfs on PATH or set KEEL_LAZYFS")

      fuse == nil ->
        skipped(:lazyfs, "fusermount3 is not installed. Install the fuse3 package")

      not device? ->
        skipped(:lazyfs, "/dev/fuse is missing. Load the fuse kernel module")

      true ->
        false
    end
  end

  def power_skip do
    {output, 0} = shell("id", ["-u"])
    user = String.trim(output)
    missing = Enum.reject(@power_tools, &System.find_executable/1)

    cond do
      user != "0" ->
        skipped(:power, "The power loss tests need root. Run them as root on a spare machine")

      missing != [] ->
        skipped(:power, "These tools are missing: #{Enum.join(missing, ", ")}")

      true ->
        false
    end
  end

  # A skip that nobody asked to see stays quiet. `--only` makes the reason print
  defp skipped(tag, message) do
    configuration = ExUnit.configuration()
    included = Keyword.get(configuration, :include, [])
    asked? = tag in included or List.keymember?(included, tag, 0)

    if asked?, do: Mix.shell().info("#{tag} tests skipped: #{message}")

    message
  end

  def power_seconds do
    text = System.get_env("KEEL_POWER_SECONDS", "600")

    String.to_integer(text)
  end

  # Runs on the peer node. The disk it builds never syncs
  def start_group_without_syncs(group, opts) do
    disk = Stub.new(:no_sync)
    given = Keyword.put(opts, :disk, disk)

    Keel.start_group(group, given)
  end

  def start_lazyfs_cluster!(base) do
    ids = ~w[a b c]
    mounts = Map.new(ids, &{&1, start_lazyfs!(base, &1)})
    cluster = TestCluster.start!(base)

    on_exit(fn -> TestCluster.stop(cluster) end)

    %{cluster: cluster, mounts: mounts, base: base}
  end

  # Every member's data directory is a LazyFS mount over a backing directory
  defp start_lazyfs!(base, id) do
    name = System.get_env("KEEL_LAZYFS", "lazyfs")
    binary = System.find_executable(name)
    home = Path.join([base, "lazyfs", id])
    root = Path.join(home, "root")
    mount = Path.join(base, id)
    fifo = Path.join(home, "faults.fifo")
    config = Path.join(home, "lazyfs.toml")

    File.mkdir_p!(root)
    File.mkdir_p!(mount)
    File.write!(config, lazyfs_config(fifo))
    {_output, 0} = shell("mkfifo", [fifo])

    args = [mount, "--config-path", config, "-o", "modules=subdir", "-o", "subdir=#{root}", "-f"]
    port = Port.open({:spawn_executable, binary}, [:binary, :exit_status, args: args])

    on_exit(fn -> unmount_lazyfs(mount, port) end)
    wait_mounted!(mount)

    %{mount: mount, root: root, fifo: fifo}
  end

  defp lazyfs_config(fifo) do
    """
    [faults]
    fifo_path="#{fifo}"

    [cache]
    apply_eviction=false

    [cache.simple]
    custom_size="1gb"
    blocks_per_page=1

    [filesystem]
    log_all_operations=false
    """
  end

  defp wait_mounted!(mount), do: wait_mounted!(mount, @lazyfs_wait_ms)

  defp wait_mounted!(mount, left) when left <= 0, do: raise("#{mount} never mounted")

  defp wait_mounted!(mount, left) do
    mounts = File.read!("/proc/mounts")

    case String.contains?(mounts, " #{mount} ") do
      true ->
        :ok

      false ->
        Process.sleep(100)

        wait_mounted!(mount, left - 100)
    end
  end

  defp unmount_lazyfs(mount, port) do
    shell("fusermount3", ["-u", mount])

    try do
      Port.close(port)
    rescue
      ArgumentError -> :ok
    end
  end

  # Drops every byte no sync covers. The line must end with a newline
  def clear_lazyfs_cache!(%{fifo: fifo}) do
    task = Task.async(fn -> File.write(fifo, "lazyfs::clear-cache\n") end)

    case Task.yield(task, 10_000) do
      {:ok, :ok} -> :ok
      _stuck -> raise "LazyFS did not read the clear-cache line from #{fifo}"
    end
  end

  # SIGKILL the member, drop its unsynced bytes, wait and start it again
  def power_cycle!(cluster, mount, id) do
    dead = TestCluster.kill!(cluster, id)

    clear_lazyfs_cache!(mount)
    Process.sleep(1_000)

    restarted = TestCluster.restart!(dead, id)
    :ok = TestCluster.wait_for(restarted, &TestCluster.converged?/1, 60_000)

    restarted
  end

  def power_cycle_all!(cluster, mounts) do
    ids = Map.keys(cluster.members)
    dead = Enum.reduce(ids, cluster, &TestCluster.kill!(&2, &1))

    Enum.each(ids, &clear_lazyfs_cache!(Map.fetch!(mounts, &1)))

    restarted = Enum.reduce(ids, dead, &TestCluster.restart!(&2, &1))
    :ok = TestCluster.wait_for(restarted, &TestCluster.leader?/1, 60_000)

    restarted
  end

  # Power cycles one random member every `every_ms` until the deadline
  def power_cycle_loop!(cluster, mounts, every_ms, deadline) do
    case System.monotonic_time(:millisecond) + every_ms >= deadline do
      true ->
        cluster

      false ->
        Process.sleep(every_ms)

        id = cluster.members |> Map.keys() |> Enum.random()
        mount = Map.fetch!(mounts, id)
        cycled = power_cycle!(cluster, mount, id)

        power_cycle_loop!(cycled, mounts, every_ms, deadline)
    end
  end

  # Zeroes four bytes at the end of the data in the newest segment of a stopped member
  def tear_last_record!(%{root: root}) do
    dir = Path.join(root, "log")
    names = dir |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".seg"))
    newest = names |> Enum.sort() |> List.last()
    path = Path.join(dir, newest)
    data = File.read!(path)
    trimmed = String.trim_trailing(data, <<0>>)
    used = byte_size(trimmed)

    {:ok, file} = :file.open(path, ~w[read write raw binary]a)
    :ok = :file.pwrite(file, used - 4, <<0, 0, 0, 0>>)
    :ok = :file.close(file)

    used
  end

  def power_down!(cluster, mount, id) do
    dead = TestCluster.kill!(cluster, id)

    clear_lazyfs_cache!(mount)

    dead
  end

  # A log-writes device over two loop devices, with an ext4 file system for member a
  def start_log_device!(base) do
    dir = Path.join(base, "power")
    name = "keel-log-#{System.unique_integer([:positive])}"
    mount = Path.join(base, "a")
    device = "/dev/mapper/#{name}"
    sectors = div(@device_bytes, 512)

    File.mkdir_p!(dir)
    File.mkdir_p!(mount)

    data_loop = attach_loop!(Path.join(dir, "data.img"), @device_bytes)
    log_loop = attach_loop!(Path.join(dir, "log.img"), @log_bytes)
    logged = %{name: name, device: device, mount: mount, log_loop: log_loop, dir: dir}

    on_exit(fn -> detach_log_device(logged, data_loop) end)

    command!("dmsetup", [
      "create",
      name,
      "--table",
      "0 #{sectors} log-writes #{data_loop} #{log_loop}"
    ])

    command!("mkfs.ext4", ["-q", device])
    command!("dmsetup", ["message", name, "0", "mark", "mkfs"])
    command!("mount", [device, mount])

    logged
  end

  defp attach_loop!(image, bytes) do
    {:ok, file} = :file.open(image, ~w[write raw binary]a)
    {:ok, _position} = :file.position(file, bytes - 1)
    :ok = :file.write(file, <<0>>)
    :ok = :file.close(file)

    output = command!("losetup", ["--find", "--show", image])

    String.trim(output)
  end

  defp detach_log_device(logged, data_loop) do
    shell("umount", [logged.mount])
    shell("dmsetup", ["remove", logged.name])
    shell("losetup", ["--detach", data_loop])
    shell("losetup", ["--detach", logged.log_loop])
  end

  defp shell(name, args),
    do: System.cmd(name, args, stderr_to_stdout: true, env: [{"RELEASE_COOKIE", nil}])

  defp command!(name, args) do
    {output, status} = shell(name, args)

    if status != 0, do: raise("#{name} #{Enum.join(args, " ")} failed: #{output}")

    output
  end

  # One member, one log device, one workload and then a replay of every point
  def power_loss_run!(base, sync) do
    logged = start_log_device!(base)
    cluster = TestCluster.start!(base, ids: ["a"], sync: sync)
    deadline = System.monotonic_time(:millisecond) + power_seconds() * 1_000

    on_exit(fn -> TestCluster.stop(cluster) end)

    acked = write_marked!(cluster, logged, deadline)

    TestCluster.stop(cluster)
    stop_log_device!(logged)

    points = replay_points(acked, 200)
    checks = Enum.map(points, &replay_and_check!(logged, cluster.cluster_id, &1, sync))

    %{acked: acked, points: checks}
  end

  # One write at a time. Each acknowledgement leaves a mark in the log
  def write_marked!(cluster, logged, deadline), do: write_marked!(cluster, logged, deadline, 1)

  defp write_marked!(cluster, logged, deadline, number) do
    case System.monotonic_time(:millisecond) >= deadline do
      true ->
        number - 1

      false ->
        result = KV.put(cluster.group, "k-#{number}", "v-#{number}", timeout: 15_000)

        mark_ack!(logged, number, result)
        write_marked!(cluster, logged, deadline, number + 1)
    end
  end

  defp mark_ack!(logged, number, {:ok, _revision}),
    do: command!("dmsetup", ["message", logged.name, "0", "mark", "ack-#{number}"])

  defp mark_ack!(_logged, _number, _failed), do: :ok

  def stop_log_device!(logged) do
    command!("umount", [logged.mount])
    command!("dmsetup", ["remove", logged.name])
  end

  # Marks to stop at. Each ack mark follows a flush. `extra` entries past it fall inside the next flush
  def replay_points(acked, wanted) do
    step = max(div(acked, wanted), 1)
    marks = Enum.take_every(1..acked//1, step)

    Enum.flat_map(marks, fn mark -> [{mark, 0}, {mark, Enum.random(1..16)}] end)
  end

  # Replays the log onto a fresh device, starts member a on it and counts the acked writes it lost
  def replay_and_check!(logged, cluster_id, point, sync) do
    {mark, extra} = point
    base = Path.join(logged.dir, "replay-#{mark}-#{extra}")
    mount = Path.join(base, "a")
    image = Path.join(base, "replay.img")
    File.mkdir_p!(mount)

    loop = attach_loop!(image, @device_bytes)
    replay_to_mark!(logged, loop, mark, extra)
    command!("mount", [loop, mount])

    lost = recovered_losses(base, cluster_id, mark, sync)

    shell("umount", [mount])
    shell("losetup", ["--detach", loop])
    File.rm(image)

    %{mark: mark, extra: extra, lost: lost}
  end

  defp replay_to_mark!(logged, loop, mark, extra) do
    stop = "ack-#{mark}"

    command!("replay-log", ["--log", logged.log_loop, "--replay", loop, "--end-mark", stop])

    if extra > 0 do
      command!("replay-log", [
        "--log",
        logged.log_loop,
        "--replay",
        loop,
        "--start-mark",
        stop,
        "--limit",
        "#{extra}"
      ])
    end
  end

  # A start that fails is a loss too, because the acked writes are out of reach
  defp recovered_losses(base, cluster_id, mark, sync) do
    cluster = TestCluster.start!(base, ids: ["a"], cluster_id: cluster_id, sync: sync)
    recent = max(1, mark - 99)..mark
    older = Enum.take_random(1..mark, 100)
    keys = Enum.concat(recent, older)
    missing = Enum.reject(keys, &acked_present?(cluster, &1))

    TestCluster.stop(cluster)

    missing
  rescue
    error -> [{:recovery_failed, Exception.message(error)}]
  end

  defp acked_present?(cluster, number) do
    case KV.get(cluster.group, "k-#{number}", timeout: 15_000) do
      {:ok, %{value: value}} -> value == "v-#{number}"
      _other -> false
    end
  end
end
