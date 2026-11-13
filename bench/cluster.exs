defmodule Bench.Cluster do
  alias Keel.Common.Name

  @ids ~w[a b c]

  @fields ~w[run group dir cluster_id members raft]a
  @enforce_keys @fields
  defstruct @fields

  @type member :: %{
          id: String.t(),
          node: node(),
          peer: nil | pid(),
          os_pid: nil | String.t()
        }

  @type t :: %__MODULE__{
          run: pos_integer(),
          group: atom(),
          dir: Path.t(),
          cluster_id: binary(),
          members: %{String.t() => member()},
          raft: keyword()
        }

  @spec start!(Path.t(), keyword()) :: t()
  def start!(dir, raft) do
    ensure_distributed!()
    run = System.unique_integer([:positive])
    members = Map.new(@ids, &{&1, boot_peer!(run, &1)})

    cluster = %__MODULE__{
      run: run,
      group: Name.build("bench", :run, Integer.to_string(run)),
      dir: dir,
      cluster_id: :crypto.strong_rand_bytes(16),
      members: members,
      raft: raft
    }

    Enum.each(@ids, &start_member!(cluster, &1))
    wait!(cluster, &leader?/1, 30_000)

    {:ok, _pid} = Keel.KV.start_writer(cluster.group, nodes: nodes(cluster))

    # One writer holds one session and the default lets 16 commands wait at once
    limits = %{session_max_pending: 128}
    {:ok, _result} = Keel.KV.Writer.submit(cluster.group, {:set_limits, limits}, timeout: 15_000)

    cluster
  end

  @spec stop(t()) :: :ok
  def stop(%__MODULE__{} = cluster) do
    Enum.each(cluster.members, fn {_id, member} -> stop_peer(member.peer) end)

    :ok
  end

  @spec nodes(t()) :: [node()]
  def nodes(%__MODULE__{} = cluster) do
    cluster.members |> Enum.sort() |> Enum.map(fn {_id, member} -> member.node end)
  end

  @spec status(t()) :: %{String.t() => {:ok, map()} | {:error, term()}}
  def status(%__MODULE__{} = cluster) do
    Map.new(cluster.members, fn {id, member} -> {id, member_status(cluster, member)} end)
  end

  @spec leader_of(map()) :: nil | String.t()
  def leader_of(statuses) do
    live = for {id, {:ok, status}} <- statuses, do: {id, status}
    top = live |> Enum.map(fn {_id, status} -> status.term end) |> Enum.max(fn -> 0 end)
    leaders = for {id, %{role: :leader, term: ^top}} <- live, do: id

    case leaders do
      [id] -> id
      _none_or_many -> nil
    end
  end

  @spec leader?(map()) :: boolean()
  def leader?(statuses), do: leader_of(statuses) != nil

  @spec converged?(map()) :: boolean()
  def converged?(statuses) do
    live = for {_id, {:ok, status}} <- statuses, do: status
    commits = live |> Enum.map(& &1.commit) |> Enum.uniq()
    applied = live |> Enum.map(& &1.applied) |> Enum.uniq()

    length(live) == map_size(statuses) and leader?(statuses) and length(commits) == 1 and
      applied == commits
  end

  @spec wait!(t(), (map() -> boolean()), pos_integer()) :: :ok
  def wait!(%__MODULE__{} = cluster, check, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    poll(cluster, check, deadline)
  end

  @spec kill!(t(), String.t()) :: t()
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

  @doc "Returns the cluster and the milliseconds the node took to boot and to recover its log."
  @spec restart!(t(), String.t()) :: {t(), map()}
  def restart!(%__MODULE__{} = cluster, id) do
    {boot_us, member} = :timer.tc(fn -> boot_peer!(cluster.run, id) end)
    restarted = %__MODULE__{cluster | members: Map.put(cluster.members, id, member)}
    {start_us, :ok} = :timer.tc(fn -> start_member!(restarted, id) end)

    {restarted, %{boot_ms: div(boot_us, 1_000), start_ms: div(start_us, 1_000)}}
  end

  @spec log_bytes(t(), String.t()) :: non_neg_integer()
  def log_bytes(%__MODULE__{} = cluster, id) do
    dir = cluster.dir |> Path.join(id) |> Path.join("log")

    dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".seg"))
    |> Enum.map(&File.stat!(Path.join(dir, &1)).size)
    |> Enum.sum()
  end

  defp poll(cluster, check, deadline) do
    statuses = status(cluster)

    cond do
      check.(statuses) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        raise "never held, last #{summary(statuses)}"

      true ->
        poll_later(cluster, check, deadline)
    end
  end

  defp summary(statuses) do
    parts =
      for {id, reply} <- Enum.sort(statuses) do
        case reply do
          {:ok, s} -> "#{id} #{s.role} term #{s.term} commit #{s.commit} applied #{s.applied}"
          {:error, reason} -> "#{id} #{inspect(reason)}"
        end
      end

    Enum.join(parts, ", ")
  end

  defp poll_later(cluster, check, deadline) do
    Process.sleep(20)

    poll(cluster, check, deadline)
  end

  defp member_status(_cluster, %{peer: nil}), do: {:error, :down}

  defp member_status(cluster, member) do
    {:ok, :erpc.call(member.node, Keel, :status, [cluster.group], 1_000)}
  catch
    class, reason -> {:error, {class, reason}}
  end

  defp start_member!(cluster, id) do
    member = Map.fetch!(cluster.members, id)
    members = Map.new(cluster.members, fn {member_id, held} -> {member_id, held.node} end)

    opts = [
      member_id: id,
      members: members,
      cluster_id: cluster.cluster_id,
      data_dir: Path.join(cluster.dir, id),
      machine: Keel.KV.Machine,
      bootstrap: true,
      raft: cluster.raft
    ]

    {:ok, _pid} = :erpc.call(member.node, Keel, :start_group, [cluster.group, opts], 120_000)

    :ok
  end

  defp boot_peer!(run, id), do: boot_peer!(run, id, 20)

  defp boot_peer!(run, id, 0), do: raise("peer #{id} of run #{run} would not start")

  defp boot_peer!(run, id, tries) do
    name = ~c"bench#{run}#{id}"

    case :peer.start(%{name: name, host: ~c"127.0.0.1", longnames: true, args: peer_args()}) do
      {:ok, peer, node} -> prepare(peer, node, id)
      {:error, _reason} -> retry_boot(run, id, tries)
    end
  end

  defp retry_boot(run, id, tries) do
    Process.sleep(100)

    boot_peer!(run, id, tries - 1)
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

  defp stop_peer(nil), do: :ok

  defp stop_peer(peer) do
    :peer.stop(peer)
  catch
    _class, _reason -> :ok
  end

  defp ensure_distributed! do
    unless Node.alive?() do
      System.cmd("epmd", ["-daemon"], env: [{"RELEASE_COOKIE", nil}])
      name = Name.build("keelbench", :node, "#{System.unique_integer([:positive])}@127.0.0.1")
      {:ok, _pid} = Node.start(name, :longnames)
    end

    :ok
  end
end
