defmodule KeelSim.Coverage do
  @moduledoc "A counter left at zero means a sweep never tested that path"

  @names ~w[
    leader_elected
    leader_stepped_down
    leader_crashed
    leadership_lost_reply
    conflict_truncation
    crash_with_pending_writes
    sync_failure_crash
    sync_failure_injected
    persist_delayed
    torn_tail_recovered
    vote_after_restart
    stale_vote_after_restart
    noop_commit
    heal_with_isolated_leader
    message_dropped
    message_duplicated
    snapshot_taken
    snapshot_installed
    log_suffix_kept
    log_suffix_dropped
    snapshot_send_restarted
    pre_vote_refused
    check_quorum_step_down
    transfer_finished
    transfer_gave_up
    batched_read
    forwarded_read
    learner_added
    learner_promoted
    voter_removed
    self_removed_leader_stepped_down
    change_in_progress_refused
    change_before_term_commit_refused
  ]a

  @type name ::
          :leader_elected
          | :leader_stepped_down
          | :leader_crashed
          | :leadership_lost_reply
          | :conflict_truncation
          | :crash_with_pending_writes
          | :sync_failure_crash
          | :sync_failure_injected
          | :persist_delayed
          | :torn_tail_recovered
          | :vote_after_restart
          | :stale_vote_after_restart
          | :noop_commit
          | :heal_with_isolated_leader
          | :message_dropped
          | :message_duplicated
          | :snapshot_taken
          | :snapshot_installed
          | :log_suffix_kept
          | :log_suffix_dropped
          | :snapshot_send_restarted
          | :pre_vote_refused
          | :check_quorum_step_down
          | :transfer_finished
          | :transfer_gave_up
          | :batched_read
          | :forwarded_read
          | :learner_added
          | :learner_promoted
          | :voter_removed
          | :self_removed_leader_stepped_down
          | :change_in_progress_refused
          | :change_before_term_commit_refused

  @type t :: %{name() => non_neg_integer()}

  @spec names() :: [name()]
  def names, do: @names

  @spec new() :: t()
  def new, do: Map.new(@names, &{&1, 0})

  @spec hit(t(), name()) :: t()
  def hit(coverage, name), do: Map.update!(coverage, name, &(&1 + 1))

  @spec merge(t(), t()) :: t()
  def merge(left, right), do: Map.merge(left, right, fn _name, a, b -> a + b end)

  @spec missing(t()) :: [name()]
  def missing(coverage) do
    for name <- @names, Map.fetch!(coverage, name) == 0, do: name
  end
end
