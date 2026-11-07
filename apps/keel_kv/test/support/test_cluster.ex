defmodule Keel.KV.TestCluster do
  alias Keel.Common.Name
  alias Keel.KV
  alias Keel.KV.Machine
  alias Keel.KV.TestUtils
  alias Keel.Node.Server

  @ids ~w[a b c]
  @raft [
    tick_ms: 20,
    heartbeat_ticks: 2,
    election_ticks_min: 10,
    election_ticks_max: 20,
    snapshot_threshold_bytes: 4_000,
    snapshot_trailing_entries: 5
  ]

  @fields ~w[run group dir cluster_id members sync]a
  @enforce_keys @fields
  defstruct @fields

  # `sync: :none` gives every member a disk that skips syncs
  def start!(dir, opts \\ []) do
    ensure_distributed!()
    ids = Keyword.get(opts, :ids, @ids)
    sync = Keyword.get(opts, :sync, :disk)
    cluster_id = Keyword.get_lazy(opts, :cluster_id, fn -> :crypto.strong_rand_bytes(16) end)
    run = System.unique_integer([:positive])
    members = Map.new(ids, &{&1, boot_peer!(run, &1)})

    cluster = %__MODULE__{
      run: run,
      group: Name.build("kc", :run, Integer.to_string(run)),
      dir: dir,
      cluster_id: cluster_id,
      members: members,
      sync: sync
    }

    Enum.each(ids, &start_member!(cluster, &1, :bootstrap))
    wait_for(cluster, &leader?/1, 15_000)

    cluster
  end

  def stop(%__MODULE__{} = cluster) do
    Enum.each(cluster.members, fn {_id, member} -> stop_peer(member.peer) end)

    writer_name = KV.Writer.name(cluster.group)
    writer = writer_name |> Process.whereis() |> writer_or_self()

    DynamicSupervisor.terminate_child(Keel.KV.Writers, writer)

    :ok
  end

  def status(%__MODULE__{} = cluster) do
    Map.new(cluster.members, fn {id, member} -> {id, member_status(cluster, member)} end)
  end

  def wait_for(%__MODULE__{} = cluster, check, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    poll(cluster, check, deadline)
  end

  def leader?(statuses), do: leader_of(statuses) != nil

  def leader_of(statuses) do
    live = for {id, {:ok, status}} <- statuses, do: {id, status}
    top = live |> Enum.map(fn {_id, status} -> status.term end) |> Enum.max(fn -> 0 end)
    leaders = for {id, %{role: :leader, term: ^top}} <- live, do: id

    case leaders do
      [id] -> id
      _none_or_many -> nil
    end
  end

  def follower_of(statuses) do
    leader = leader_of(statuses)
    live = for {id, {:ok, _status}} <- statuses, id != leader, do: id

    hd(Enum.sort(live))
  end

  def last_index_of(statuses, id) do
    {:ok, status} = Map.fetch!(statuses, id)

    status.last_index
  end

  def first_index_of(statuses, id) do
    {:ok, status} = Map.fetch!(statuses, id)

    status.first_index
  end

  def compacted_past?(statuses, index) do
    leader = leader_of(statuses)

    leader != nil and first_index_of(statuses, leader) > index + 1
  end

  def put_many!(%__MODULE__{} = cluster, count) do
    Enum.each(1..count, fn number ->
      {:ok, _revision} =
        KV.put(cluster.group, "fill-#{number}", String.duplicate("v", 60), timeout: 15_000)
    end)
  end

  def leader_changed?(statuses, old) do
    leader = leader_of(statuses)

    leader != nil and leader != old
  end

  def converged?(statuses) do
    live = for {_id, {:ok, status}} <- statuses, do: status
    leaders = live |> Enum.map(& &1.leader) |> Enum.uniq()
    commits = live |> Enum.map(& &1.commit) |> Enum.uniq()
    applied = live |> Enum.map(& &1.applied) |> Enum.uniq()

    length(live) == map_size(statuses) and length(leaders) == 1 and hd(leaders) != nil and
      length(commits) == 1 and applied == commits
  end

  def voters_of(statuses, id) do
    {:ok, status} = Map.fetch!(statuses, id)

    status.voters
  end

  def learners_of(statuses, id) do
    {:ok, status} = Map.fetch!(statuses, id)

    status.learners
  end

  def voted?(statuses, ids) do
    live = for {_id, {:ok, status}} <- statuses, do: status.voters

    length(live) == map_size(statuses) and Enum.all?(live, &(&1 == Enum.sort(ids)))
  end

  def caught_up?(statuses, id) do
    leader = leader_of(statuses)

    with {:ok, status} <- Map.fetch!(statuses, id),
         {:ok, head} <- Map.fetch!(statuses, leader) do
      status.applied == head.commit and status.commit == head.commit
    else
      _other -> false
    end
  end

  def snapshotted?(statuses, id) do
    {:ok, status} = Map.fetch!(statuses, id)

    status.first_index > 1
  end

  def join!(%__MODULE__{} = cluster, id) do
    member = boot_peer!(cluster.run, id)
    joining = %__MODULE__{cluster | members: Map.put(cluster.members, id, member)}

    start_member!(joining, id, :join)

    joining
  end

  def add_member!(%__MODULE__{} = cluster, via, id, role) do
    node = node_of(cluster, via)
    member = node_of(cluster, id)

    :erpc.call(node, Keel, :add_member, [cluster.group, id, member, role], 60_000)
  end

  def promote_member!(%__MODULE__{} = cluster, via, id) do
    node = node_of(cluster, via)

    :erpc.call(node, Keel, :promote_member, [cluster.group, id], 60_000)
  end

  def remove_member!(%__MODULE__{} = cluster, via, id) do
    node = node_of(cluster, via)

    :erpc.call(node, Keel, :remove_member, [cluster.group, id], 60_000)
  end

  def drop!(%__MODULE__{} = cluster, id) do
    member = Map.fetch!(cluster.members, id)

    stop_peer(member.peer)

    %__MODULE__{cluster | members: Map.delete(cluster.members, id)}
  end

  def kill!(%__MODULE__{} = cluster, id) do
    member = Map.fetch!(cluster.members, id)
    ref = Process.monitor(member.peer)

    {_output, 0} = System.cmd("kill", ["-9", member.os_pid], env: [{"RELEASE_COOKIE", nil}])

    receive do
      {:DOWN, ^ref, :process, _pid, _reason} -> :ok
    after
      10_000 -> raise "member #{id} did not stop"
    end

    dead = %{member | peer: nil, os_pid: nil}

    %__MODULE__{cluster | members: Map.put(cluster.members, id, dead)}
  end

  def restart!(%__MODULE__{} = cluster, id) do
    member = boot_peer!(cluster.run, id)
    restarted = %__MODULE__{cluster | members: Map.put(cluster.members, id, member)}

    start_member!(restarted, id, :bootstrap)

    restarted
  end

  def partition!(%__MODULE__{} = cluster, side) do
    others = Map.keys(cluster.members) -- side

    Enum.each(side, &faults!(cluster, &1, :partition, [others]))
    Enum.each(others, &faults!(cluster, &1, :partition, [side]))
  end

  def heal!(%__MODULE__{} = cluster) do
    live = for {id, %{peer: peer}} <- cluster.members, peer != nil, do: id

    Enum.each(live, &faults!(cluster, &1, :clear, []))
  end

  def transfer!(%__MODULE__{} = cluster, from, to) do
    node = node_of(cluster, from)

    :erpc.call(node, Keel, :transfer_leadership, [cluster.group, to], 10_000)
  end

  def read_at(%__MODULE__{} = cluster, id, query) do
    node = node_of(cluster, id)
    name = Server.name(cluster.group)

    :erpc.call(node, Server, :read, [name, query, 5_000], 10_000)
  end

  def led_by?(statuses, id), do: leader_of(statuses) == id

  def start_writes!(%__MODULE__{} = cluster, count, notify) do
    Task.async(fn -> Enum.map(1..count, &write_one(cluster.group, &1, notify)) end)
  end

  # Writes in a row until the deadline, in milliseconds on the monotonic clock
  def start_writes_until!(%__MODULE__{} = cluster, deadline) do
    Task.async(fn -> write_until(cluster.group, deadline, 1, []) end)
  end

  def lost(results, %__MODULE__{} = cluster) do
    for {key, value, {:ok, _revision}} <- results, not holds?(cluster.group, key, value), do: key
  end

  def acknowledged(results),
    do: Enum.count(results, &match?({_key, _value, {:ok, _revision}}, &1))

  def node_of(%__MODULE__{} = cluster, id), do: Map.fetch!(cluster.members, id).node

  defp write_one(group, number, notify) do
    key = "key-#{number}"
    value = "value-#{number}-#{:erlang.unique_integer([:positive])}"
    result = KV.put(group, key, value, timeout: 15_000)

    if match?({:ok, _revision}, result), do: send(notify, {:acked, key})

    {key, value, result}
  end

  defp write_until(group, deadline, number, results) do
    case System.monotonic_time(:millisecond) >= deadline do
      true ->
        Enum.reverse(results)

      false ->
        result = write_one(group, number, self())

        write_until(group, deadline, number + 1, [result | results])
    end
  end

  defp holds?(group, key, value) do
    case KV.get(group, key, timeout: 15_000) do
      {:ok, %{value: ^value}} -> true
      _other -> false
    end
  end

  defp poll(cluster, check, deadline) do
    statuses = status(cluster)

    cond do
      check.(statuses) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        raise "never held, last seen #{inspect(statuses)}"

      true ->
        Process.sleep(50) && poll(cluster, check, deadline)
    end
  end

  defp member_status(_cluster, %{peer: nil}), do: {:error, :down}

  defp member_status(cluster, member) do
    {:ok, :erpc.call(member.node, Keel, :status, [cluster.group], 1_000)}
  catch
    class, reason -> {:error, {class, reason}}
  end

  defp faults!(cluster, id, function, args) do
    node = node_of(cluster, id)

    :erpc.call(node, Keel.Transport.FaultProxy, function, [cluster.group | args], 5_000)
  end

  defp start_member!(cluster, id, mode) do
    node = node_of(cluster, id)
    members = Map.new(cluster.members, fn {member_id, member} -> {member_id, member.node} end)

    opts = [
      member_id: id,
      members: members,
      cluster_id: cluster.cluster_id,
      data_dir: Path.join(cluster.dir, id),
      machine: Machine,
      transport: Keel.Transport.FaultProxy,
      transport_opts: [inner: Keel.Transport.Dist],
      raft: @raft
    ]

    started = [{mode_flag(mode), true} | opts]

    {:ok, _pid} = start_group!(cluster.sync, node, cluster.group, started)

    :ok
  end

  defp start_group!(:disk, node, group, opts),
    do: :erpc.call(node, Keel, :start_group, [group, opts], 10_000)

  defp start_group!(:none, node, group, opts),
    do: :erpc.call(node, TestUtils, :start_group_without_syncs, [group, opts], 10_000)

  defp mode_flag(:bootstrap), do: :bootstrap
  defp mode_flag(:join), do: :join

  defp boot_peer!(run, id), do: boot_peer!(run, id, 20)

  defp boot_peer!(run, id, 0), do: raise("peer #{id} of run #{run} would not start")

  defp boot_peer!(run, id, tries) do
    name = ~c"kc#{run}#{id}"

    case :peer.start(%{name: name, host: ~c"127.0.0.1", longnames: true, args: peer_args()}) do
      {:ok, peer, node} -> prepare(peer, node, id)
      {:error, _reason} -> Process.sleep(100) && boot_peer!(run, id, tries - 1)
    end
  end

  defp prepare(peer, node, id) do
    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:keel_kv])
    :ok = :erpc.call(node, Logger, :configure, [[level: :error]])
    os_pid = :erpc.call(node, :os, :getpid, [])

    %{id: id, node: node, peer: peer, os_pid: List.to_string(os_pid)}
  end

  defp peer_args do
    cookie = Node.get_cookie()
    paths = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)

    [~c"-setcookie", Atom.to_charlist(cookie) | paths]
  end

  defp writer_or_self(nil), do: self()
  defp writer_or_self(pid), do: pid

  defp stop_peer(nil), do: :ok

  defp stop_peer(peer) do
    :peer.stop(peer)
  catch
    _class, _reason -> :ok
  end

  defp ensure_distributed! do
    unless Node.alive?() do
      System.cmd("epmd", ["-daemon"], env: [{"RELEASE_COOKIE", nil}])
      name = Name.build("keeltest", :node, "#{System.unique_integer([:positive])}@127.0.0.1")
      {:ok, _pid} = Node.start(name, :longnames)
    end

    :ok
  end
end
