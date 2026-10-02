defmodule KeelSim.Invariants do
  alias Keel.Chain.Machine
  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.Log
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Node
  alias Keel.Raft.SnapshotMeta
  alias KeelSim.KvCheck

  defstruct leaders: %{},
            durable_votes: %{},
            committed: %{},
            chain: %{},
            times: %{},
            commit_terms: %{},
            results: %{},
            digests: %{},
            violations: [],
            commit_configs: %{},
            removed: MapSet.new(),
            elections: 0,
            newest_commit_term: 0

  @type violation ::
          {:election_safety, T.term_number(), [T.member_id()]}
          | {:vote_changed, T.member_id(), T.term_number()}
          | {:state_machine_safety, T.member_id(), T.index()}
          | {:chain_diverged, T.member_id(), non_neg_integer()}
          | {:time_diverged, T.member_id(), T.index()}
          | {:time_regressed, T.member_id(), T.index()}
          | {:result_diverged, T.member_id(), T.index()}
          | {:state_diverged, T.member_id(), T.index()}
          | {:snapshot_diverged, T.member_id(), T.index()}
          | {:snapshot_log_mismatch, T.member_id(), T.index()}
          | {:leader_completeness, T.member_id(), T.index()}
          | {:vote_lost, T.member_id(), T.term_number()}
          | {:acknowledged_loss, T.index()}
          | {:recovery_failed, T.member_id(), term()}
          | {:term_regressed, T.member_id(), T.term_number(), T.term_number()}
          | {:raised, atom()}
          | {:stale_leader_read, T.member_id(), T.term_number()}
          | {:removed_rejoined, T.member_id()}
          | {:ack_not_durable, T.member_id(), T.index()}
          | KvCheck.violation()

  @type durable_log ::
          {:ok, %{T.index() => {T.term_number(), binary()}}} | {:error, term()}

  @type t :: %__MODULE__{
          leaders: %{T.term_number() => T.member_id()},
          durable_votes: %{{T.member_id(), T.term_number()} => T.member_id()},
          committed: %{T.index() => {T.term_number(), binary()}},
          chain: %{non_neg_integer() => binary()},
          times: %{T.index() => non_neg_integer()},
          commit_terms: %{T.index() => T.term_number()},
          results: %{T.index() => Keel.Machine.result()},
          digests: %{T.index() => binary()},
          violations: [violation()],
          commit_configs: %{T.index() => {MapSet.t(T.member_id()), pos_integer()}},
          removed: MapSet.t(T.member_id()),
          elections: non_neg_integer(),
          newest_commit_term: T.term_number()
        }

  @spec leader_elected(t(), T.member_id(), T.term_number()) :: t()
  def leader_elected(%__MODULE__{} = invariants, id, term) do
    elected = %{invariants | elections: invariants.elections + 1}

    case Map.fetch(invariants.leaders, term) do
      :error -> %{elected | leaders: Map.put(invariants.leaders, term, id)}
      {:ok, ^id} -> elected
      {:ok, other} -> election_safety(elected, term, id, other)
    end
  end

  @spec vote_durable(t(), T.member_id(), nil | HardState.t()) :: t()
  def vote_durable(%__MODULE__{} = invariants, _id, nil), do: invariants
  def vote_durable(%__MODULE__{} = invariants, _id, %HardState{voted_for: nil}), do: invariants

  def vote_durable(%__MODULE__{} = invariants, id, %HardState{} = hard_state) do
    %HardState{term: term, voted_for: vote} = hard_state
    key = {id, term}

    case Map.fetch(invariants.durable_votes, key) do
      :error -> %{invariants | durable_votes: Map.put(invariants.durable_votes, key, vote)}
      {:ok, ^vote} -> invariants
      {:ok, _other} -> violate(invariants, {:vote_changed, id, term})
    end
  end

  @spec term_stepped(t(), T.member_id(), T.term_number(), T.term_number()) :: t()
  def term_stepped(%__MODULE__{} = invariants, _id, before, later) when later >= before,
    do: invariants

  def term_stepped(%__MODULE__{} = invariants, id, before, later),
    do: violate(invariants, {:term_regressed, id, before, later})

  @doc "An ack names entries the member holds on disk. A newer term in that place means they were replaced."
  @spec acks_sent(t(), T.member_id(), Node.t(), [Node.effect()]) :: t()
  def acks_sent(%__MODULE__{} = invariants, id, %Node{} = node, effects),
    do: Enum.reduce(effects, invariants, &ack_sent(&2, id, node, &1))

  @spec raised(t(), atom()) :: t()
  def raised(%__MODULE__{} = invariants, invariant), do: violate(invariants, {:raised, invariant})

  @spec vote_recovered(t(), T.member_id(), HardState.t()) :: t()
  def vote_recovered(%__MODULE__{} = invariants, id, %HardState{} = recovered) do
    lost =
      invariants.durable_votes
      |> Enum.sort()
      |> Enum.find(fn {{member, term}, vote} ->
        member == id and forgotten?(recovered, term, vote)
      end)

    case lost do
      nil -> invariants
      {{_member, term}, _vote} -> violate(invariants, {:vote_lost, id, term})
    end
  end

  @spec recovery_failed(t(), T.member_id(), term()) :: t()
  def recovery_failed(%__MODULE__{} = invariants, id, reason),
    do: violate(invariants, {:recovery_failed, id, reason})

  @doc "Each log is what that member would recover after losing all unsynced data."
  @spec acknowledged(t(), %{T.member_id() => durable_log()}, pos_integer()) :: t()
  def acknowledged(%__MODULE__{} = invariants, logs, quorum) do
    failed = logs |> Enum.sort() |> Enum.find(&match?({_id, {:error, _reason}}, &1))

    case failed do
      {id, {:error, reason}} -> recovery_failed(invariants, id, reason)
      nil -> lost_entry(invariants, logs, quorum)
    end
  end

  @doc "The first member to apply an entry holds the config that its leader counted the quorum in."
  @spec committed_with(t(), T.index(), Config.t()) :: t()
  def committed_with(%__MODULE__{} = invariants, index, %Config{} = config) do
    held = {config.voters, Config.quorum(config)}

    %{invariants | commit_configs: Map.put_new(invariants.commit_configs, index, held)}
  end

  @doc "An id that an earlier config removed never appears in a later one. Only the first apply of an entry counts."
  @spec config_applied(t(), T.member_id(), Entry.t()) :: t()
  def config_applied(%__MODULE__{commit_configs: held} = invariants, _id, %Entry{} = entry)
      when is_map_key(held, entry.index),
      do: invariants

  def config_applied(%__MODULE__{} = invariants, _id, %Entry{kind: :config, data: data}) do
    config = Config.decode!(data)
    members = config |> Config.members() |> MapSet.new()

    checked =
      invariants.removed
      |> MapSet.intersection(members)
      |> Enum.sort()
      |> Enum.reduce(invariants, &violate(&2, {:removed_rejoined, &1}))

    %{checked | removed: MapSet.union(checked.removed, config.removed)}
  end

  def config_applied(%__MODULE__{} = invariants, _id, %Entry{}), do: invariants

  @doc "An entry carries the same time on every member. Log time never goes down from one index to the next."
  @spec applied(t(), T.member_id(), Entry.t()) :: t()
  def applied(%__MODULE__{} = invariants, id, %Entry{} = entry) do
    seen = {entry.term, entry.data}

    checked =
      case Map.fetch(invariants.committed, entry.index) do
        :error -> %{invariants | committed: Map.put(invariants.committed, entry.index, seen)}
        {:ok, ^seen} -> invariants
        {:ok, _other} -> violate(invariants, {:state_machine_safety, id, entry.index})
      end

    timed(checked, id, entry)
  end

  @spec chained(t(), T.member_id(), Machine.t()) :: t()
  def chained(%__MODULE__{} = invariants, id, %Machine{count: count, hash: hash}) do
    case Map.fetch(invariants.chain, count) do
      :error -> %{invariants | chain: Map.put(invariants.chain, count, hash)}
      {:ok, ^hash} -> invariants
      {:ok, _other} -> violate(invariants, {:chain_diverged, id, count})
    end
  end

  @spec result_applied(t(), T.member_id(), T.index(), Keel.Machine.result()) :: t()
  def result_applied(%__MODULE__{} = invariants, id, index, result) do
    case Map.fetch(invariants.results, index) do
      :error -> %{invariants | results: Map.put(invariants.results, index, result)}
      {:ok, ^result} -> invariants
      {:ok, _other} -> violate(invariants, {:result_diverged, id, index})
    end
  end

  @spec state_applied(t(), T.member_id(), T.index(), binary()) :: t()
  def state_applied(%__MODULE__{} = invariants, id, index, digest) do
    case Map.fetch(invariants.digests, index) do
      :error -> %{invariants | digests: Map.put(invariants.digests, index, digest)}
      {:ok, ^digest} -> invariants
      {:ok, _other} -> violate(invariants, {:state_diverged, id, index})
    end
  end

  @doc "A snapshot at an index holds the state every member had there. A restored state is checked the same way."
  @spec snapshot_state(t(), T.member_id(), T.index(), binary()) :: t()
  def snapshot_state(%__MODULE__{} = invariants, id, index, digest) do
    case Map.fetch(invariants.digests, index) do
      :error -> %{invariants | digests: Map.put(invariants.digests, index, digest)}
      {:ok, ^digest} -> invariants
      {:ok, _other} -> violate(invariants, {:snapshot_diverged, id, index})
    end
  end

  @doc "After an install the log must answer with the term of the snapshot at its index."
  @spec snapshot_installed(t(), T.member_id(), Log.t(), SnapshotMeta.t()) :: t()
  def snapshot_installed(%__MODULE__{} = invariants, id, %Log{} = log, %SnapshotMeta{} = meta) do
    if Log.term_at(log, meta.index) == meta.term,
      do: invariants,
      else: violate(invariants, {:snapshot_log_mismatch, id, meta.index})
  end

  @spec history_checked(t(), [violation()]) :: t()
  def history_checked(%__MODULE__{} = invariants, violations),
    do: Enum.reduce(violations, invariants, &violate(&2, &1))

  @doc "The first member to apply an entry is in the term of the leader that committed it."
  @spec committed_in(t(), T.index(), T.term_number()) :: t()
  def committed_in(%__MODULE__{} = invariants, index, term) do
    terms = Map.put_new(invariants.commit_terms, index, term)

    %{
      invariants
      | commit_terms: terms,
        newest_commit_term: max(invariants.newest_commit_term, term)
    }
  end

  @doc "A leader never confirms a read that began after a newer leader committed."
  @spec read_served(t(), T.member_id(), T.term_number(), T.term_number()) :: t()
  def read_served(%__MODULE__{} = invariants, id, leader_term, newest_at_start) do
    if newest_at_start > leader_term,
      do: violate(invariants, {:stale_leader_read, id, leader_term}),
      else: invariants
  end

  @doc "A new leader holds every entry committed in a lower term."
  @spec leader_log(t(), T.member_id(), T.term_number(), Log.t()) :: t()
  def leader_log(%__MODULE__{} = invariants, id, leader_term, %Log{} = log) do
    missing =
      invariants.committed
      |> Enum.sort()
      |> Enum.find(fn {index, {term, _data}} ->
        index > log.snapshot_index and Map.get(invariants.commit_terms, index, 0) < leader_term and
          Log.term_at(log, index) != term
      end)

    case missing do
      nil -> invariants
      {index, _seen} -> violate(invariants, {:leader_completeness, id, index})
    end
  end

  defp timed(invariants, id, %Entry{index: index, time_ms: time_ms}) do
    case Map.fetch(invariants.times, index) do
      :error -> first_time(invariants, id, index, time_ms)
      {:ok, ^time_ms} -> invariants
      {:ok, _other} -> violate(invariants, {:time_diverged, id, index})
    end
  end

  defp first_time(invariants, id, index, time_ms) do
    noted = %{invariants | times: Map.put(invariants.times, index, time_ms)}

    if Map.get(invariants.times, index - 1, 0) > time_ms,
      do: violate(noted, {:time_regressed, id, index}),
      else: noted
  end

  defp election_safety(invariants, term, id, other) do
    leaders = Enum.sort([id, other])

    violate(invariants, {:election_safety, term, leaders})
  end

  defp forgotten?(%HardState{term: term}, voted, _vote) when term > voted, do: false
  defp forgotten?(%HardState{term: term, voted_for: vote}, term, vote), do: false
  defp forgotten?(%HardState{}, _term, _vote), do: true

  # The quorum is the one of the config the leader counted in when it committed the entry
  defp lost_entry(invariants, member_logs, quorum) do
    logs = Map.new(member_logs, fn {id, {:ok, log}} -> {id, log} end)

    lost =
      invariants.committed
      |> Enum.sort()
      |> Enum.find(fn {index, seen} -> lost?(invariants, logs, index, seen, quorum) end)

    case lost do
      nil -> invariants
      {index, _seen} -> violate(invariants, {:acknowledged_loss, index})
    end
  end

  defp lost?(invariants, logs, index, seen, quorum) do
    default = {Map.keys(logs), quorum}
    {voters, needed} = Map.get(invariants.commit_configs, index, default)
    holders = for id <- voters, log = logs[id], Map.get(log, index) == seen, do: id

    length(holders) < needed
  end

  defp ack_sent(invariants, id, node, {:send, %AppendEntriesReply{success?: true} = reply}) do
    index = reply.match_index

    case durable_ack?(node, index, reply.term) do
      true -> invariants
      false -> violate(invariants, {:ack_not_durable, id, index})
    end
  end

  defp ack_sent(invariants, _id, _node, _effect), do: invariants

  # Compacted entries are committed so they cannot be replaced
  defp durable_ack?(%Node{log: log}, index, _term) when index < log.first_index, do: true
  defp durable_ack?(%Node{durable_index: durable}, index, _term) when index > durable, do: false
  defp durable_ack?(%Node{log: log}, index, term), do: Log.term_at(log, index) <= term

  defp violate(invariants, violation) do
    if violation in invariants.violations,
      do: invariants,
      else: %{invariants | violations: [violation | invariants.violations]}
  end
end
