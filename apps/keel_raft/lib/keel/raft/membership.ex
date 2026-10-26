defmodule Keel.Raft.Membership do
  @moduledoc "A leader starts a change only after an entry of its own term is committed"

  alias Keel.Raft.Config
  alias Keel.Raft.Durability
  alias Keel.Raft.Election
  alias Keel.Raft.Entry
  alias Keel.Raft.Log
  alias Keel.Raft.Node
  alias Keel.Raft.Replication
  alias Keel.Raft.Snapshot

  @spec change(Node.t(), reference(), Config.change()) :: {Node.t(), [Node.effect()]}
  def change(%Node{role: :leader} = node, ref, change) do
    with :ok <- no_transfer(node),
         :ok <- term_entry_committed(node),
         :ok <- no_change_in_flight(node),
         :ok <- learner_caught_up(node, change),
         {:ok, config} <- Config.apply_change(node.config, change) do
      append_config(node, ref, config)
    else
      {:error, reason} -> {node, [{:reply, ref, {:error, reason}}]}
    end
  end

  def change(%Node{} = node, ref, _change),
    do: {node, [{:reply, ref, {:error, {:not_leader, node.leader_id}}}]}

  @doc "The config is the newest config entry of the log. Call it after the log changes."
  @spec refresh(Node.t()) :: Node.t()
  def refresh(%Node{} = node), do: %{node | config: Config.current(node.log)}

  @doc "A leader that is no longer a member steps down once the entry that removed it commits."
  @spec committed(Node.t()) :: {Node.t(), [Node.effect()]}
  def committed(%Node{role: :leader, leader: %{pending_config_index: pending}} = node)
      when pending <= node.commit do
    pruned = prune(node)

    if Config.member?(pruned.config, pruned.id),
      do: {pruned, []},
      else: Election.step_down(pruned, pruned.term, nil)
  end

  def committed(%Node{} = node), do: {node, []}

  defp no_transfer(%Node{leader: %{transfer: nil}}), do: :ok
  defp no_transfer(%Node{}), do: {:error, :transfer_in_progress}

  defp term_entry_committed(%Node{opts: %{bug: :change_without_term_commit}}), do: :ok

  defp term_entry_committed(%Node{commit: commit, leader: %{term_start_index: start}})
       when commit >= start,
       do: :ok

  defp term_entry_committed(%Node{}), do: {:error, :not_ready}

  defp no_change_in_flight(%Node{commit: commit, leader: %{pending_config_index: pending}})
       when pending <= commit,
       do: :ok

  defp no_change_in_flight(%Node{}), do: {:error, :change_in_progress}

  defp learner_caught_up(%Node{} = node, {:promote, id}) do
    case Map.fetch(node.leader.progress, id) do
      {:ok, progress} -> check_lag(node, id, progress)
      :error -> :ok
    end
  end

  defp learner_caught_up(%Node{}, _change), do: :ok

  defp check_lag(node, id, progress) do
    lag = node.log.last_index - progress.match_index
    recent? = MapSet.member?(node.leader.active, id)

    if recent? and lag <= node.opts.promote_lag_entries,
      do: :ok,
      else: {:error, :not_caught_up}
  end

  defp append_config(%Node{} = node, ref, config) do
    entry = %Entry{
      term: node.term,
      index: node.log.last_index + 1,
      kind: :config,
      data: Config.encode(config),
      time_ms: node.log.last_time_ms
    }

    pending = Map.put(node.leader.pending, entry.index, ref)
    progress = with_new_members(node, config, entry)

    leader = %{
      node.leader
      | pending: pending,
        pending_config_index: entry.index,
        progress: progress
    }

    appended = %{node | log: Log.append(node.log, [entry]), config: config, leader: leader}
    tracked = Snapshot.track(appended, [entry])
    {persisting, persist} = Durability.persist(tracked, false, [entry])
    {sent, appends} = Replication.replicate_all(persisting)

    {sent, [persist | appends]}
  end

  # A removed member keeps its progress until the entry commits so it still hears of it
  defp with_new_members(%Node{} = node, config, %Entry{} = entry) do
    fresh = %{next_index: entry.index, match_index: 0, seq: 0, pipelining?: false, snapshot: nil}

    config
    |> Config.members()
    |> Enum.reject(&(&1 == node.id or Map.has_key?(node.leader.progress, &1)))
    |> Enum.reduce(node.leader.progress, &Map.put(&2, &1, fresh))
  end

  defp prune(%Node{leader: leader} = node) do
    others = node.config |> Config.members() |> List.delete(node.id)
    kept = Map.take(leader.progress, others)

    %{node | leader: %{leader | progress: kept}}
  end
end
