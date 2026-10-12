defmodule KeelSim.Liveness do
  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Node

  @type kind :: :no_leader | :no_commit | :not_converged

  @type member :: %{node: nil | Node.t(), machine: term(), applied: T.index()}

  @type members :: %{T.member_id() => member()}

  @type status :: :ok | {:pending, kind()}

  @type leader_result :: {:ok, Node.t()} | {:pending, :no_leader}

  @doc "States are compared after `canonical` turns each one into a form that equal states share."
  @spec status(members(), [T.member_id()], boolean(), (term() -> term())) :: status()
  def status(members, ids, committed?, canonical \\ & &1) do
    with {:ok, leader} <- leader(members, ids),
         :ok <- progress(committed?) do
      converged(members, participants(leader, ids), leader, canonical)
    end
  end

  @spec leader(members(), [T.member_id()]) :: leader_result()
  def leader(members, ids) do
    up = for id <- ids, node = members[id].node, do: node
    top = up |> Enum.map(& &1.term) |> Enum.max(fn -> 0 end)

    with %Node{} = leader <- Enum.find(up, &(&1.role == :leader and &1.term == top)),
         true <- following(up, leader, top) >= Config.quorum(leader.config) do
      {:ok, leader}
    else
      _missing -> {:pending, :no_leader}
    end
  end

  # Only the voters of the leader's own config count
  defp following(up, leader, top) do
    Enum.count(up, fn node ->
      node.term == top and node.leader_id == leader.id and Config.voter?(leader.config, node.id)
    end)
  end

  # A joiner that nobody added and a member that was removed are not part of the check
  defp participants(%Node{config: config}, ids), do: Enum.filter(ids, &Config.member?(config, &1))

  defp progress(true), do: :ok
  defp progress(false), do: {:pending, :no_commit}

  defp converged(members, ids, leader, canonical) do
    applied = Enum.map(ids, &members[&1].applied)
    commits = for id <- ids, node = members[id].node, do: node.commit
    all_up? = length(commits) == length(ids)
    commits_equal? = Enum.all?(commits, &(&1 == leader.commit))
    applied_all? = Enum.all?(applied, &(&1 == leader.commit))

    if all_up? and commits_equal? and applied_all? and same_machines?(members, ids, canonical),
      do: :ok,
      else: {:pending, :not_converged}
  end

  defp same_machines?(members, ids, canonical) do
    machines = Enum.map(ids, &canonical.(members[&1].machine))

    Enum.all?(machines, &(&1 == hd(machines)))
  end
end
