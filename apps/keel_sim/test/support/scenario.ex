defmodule KeelSim.Scenario do
  alias Keel.Raft.Log
  alias KeelSim.Cluster
  alias KeelSim.Network

  def play(script, opts) do
    base = %{
      seed: 1,
      steps: 0,
      faults: [],
      fault_rate: 0,
      crash_profile: :strict,
      workload: :chain,
      pre_vote: false,
      bootstrap: false,
      snapshot_threshold_bytes: 1_000_000_000
    }

    timers = %{election_ticks_min: 10, election_ticks_max: 20, heartbeat_ticks: 2}
    config = base |> Map.merge(timers) |> Map.merge(opts)
    cluster = Cluster.new(config)

    Enum.reduce(script, cluster, &step(&2, &1))
  end

  def finish(cluster) do
    healed = Cluster.verify(cluster)

    Cluster.result(healed)
  end

  def log_times(script, opts) do
    cluster = play(script, opts)
    longest = Enum.max_by(cluster.ids, &node(cluster, &1).log.last_index)
    log = node(cluster, longest).log

    Enum.map(1..log.last_index, &Log.time_at(log, &1))
  end

  def sorted?(times), do: times == Enum.sort(times)

  def node(cluster, id), do: cluster.members[id].node

  def holders(cluster, index, term) do
    for id <- cluster.ids,
        node = cluster.members[id].node,
        Log.term_at(node.log, index) == term do
      id
    end
  end

  defp step(cluster, {:elect, id, voters}), do: elect(cluster, id, voters)
  defp step(cluster, {:campaign, id}), do: campaign(cluster, id)
  defp step(cluster, {:exchange, first, second}), do: exchange(cluster, first, second)
  defp step(cluster, {:propose, id, data}), do: Cluster.act(cluster, {:propose, id, data})
  defp step(cluster, {:sync, id}), do: sync(cluster, id)
  defp step(cluster, {:tick, id, count}), do: tick(cluster, id, count)
  defp step(cluster, {:deliver, from, to}), do: Cluster.act(cluster, {:deliver, {from, to}})
  defp step(cluster, {:deliver_all, from, to}), do: all(cluster, :deliver, {from, to})
  defp step(cluster, {:drop, from, to}), do: Cluster.act(cluster, {:drop, {from, to}})
  defp step(cluster, {:drop_all, from, to}), do: all(cluster, :drop, {from, to})
  defp step(cluster, {:crash, id}), do: Cluster.act(cluster, {:crash, id})
  defp step(cluster, {:restart, id}), do: Cluster.act(cluster, {:restart, id})
  defp step(cluster, {:snapshot, id}), do: Cluster.act(cluster, {:snapshot_step, id})
  defp step(cluster, {:isolate, id}), do: Cluster.act(cluster, {:isolate, id})
  defp step(cluster, :heal), do: Cluster.act(cluster, :heal)
  defp step(cluster, {:pause, id}), do: Cluster.act(cluster, {:pause, id})
  defp step(cluster, {:resume, id}), do: Cluster.act(cluster, {:resume, id})
  defp step(cluster, {:transfer, id, target}), do: Cluster.act(cluster, {:transfer, id, target})
  defp step(cluster, {:join, id}), do: Cluster.act(cluster, {:join, id})
  defp step(cluster, {:change, id, change}), do: Cluster.act(cluster, {:change, id, change})

  defp step(cluster, {:client, slot, kind, key, target}),
    do: Cluster.act(cluster, {:client, slot, {kind, key, target}})

  defp step(cluster, {:expect_holders, index, term, count}) do
    held = holders(cluster, index, term)
    message = "holders of #{index}: #{inspect(held)}"

    if length(held) == count, do: cluster, else: raise(message)
  end

  defp tick(cluster, _id, 0), do: cluster

  defp tick(cluster, id, count) do
    ticked = Cluster.act(cluster, {:tick, id})

    tick(ticked, id, count - 1)
  end

  defp campaign(cluster, id), do: campaign(cluster, id, [])

  defp campaign(cluster, id, voters) do
    term = node(cluster, id).term

    campaign_again(cluster, id, voters, term, 100)
  end

  defp campaign_again(_cluster, id, _voters, _term, 0), do: raise("#{id} never campaigned")

  defp campaign_again(cluster, id, voters, term, left) do
    ticked = Cluster.act(cluster, {:tick, id})
    asked = ask_pre_vote(ticked, id, voters)
    campaigning? = node(asked, id).term > term

    if campaigning?, do: asked, else: campaign_again(asked, id, voters, term, left - 1)
  end

  # A pre-vote round needs its answers before the term can move
  defp ask_pre_vote(cluster, id, voters) do
    if node(cluster, id).role == :pre_candidate,
      do: Enum.reduce(voters, cluster, &exchange(&2, id, &1)),
      else: cluster
  end

  defp elect(cluster, id, voters) do
    campaigned = campaign(cluster, id, voters)
    others = campaigned.ids -- [id | voters]
    cut = Enum.reduce(others, campaigned, &all(&2, :drop, {id, &1}))
    synced = sync(cut, id)
    gathered = Enum.reduce(voters, synced, &exchange(&2, id, &1))
    leader = node(gathered, id)

    if leader.role == :leader, do: gathered, else: raise("#{id} was not elected")
  end

  defp exchange(cluster, first, second) do
    sent = all(cluster, :deliver, {first, second})
    synced = sync_if_pending(sent, second)

    all(synced, :deliver, {second, first})
  end

  defp sync(cluster, id), do: Cluster.act(cluster, {:complete_persist, id, :sync})

  defp sync_if_pending(cluster, id) do
    if cluster.members[id].syncing, do: sync(cluster, id), else: cluster
  end

  defp all(cluster, kind, link) do
    ready = Network.ready_links(cluster.network)

    if link in ready do
      acted = Cluster.act(cluster, {kind, link})

      all(acted, kind, link)
    else
      cluster
    end
  end
end
