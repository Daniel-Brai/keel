defmodule Keel.Error do
  use Boundary, top_level?: true, deps: [], exports: []

  @type hint :: nil | {String.t(), node()}

  @type reason ::
          {:not_leader, hint()}
          | :no_leader
          | :leadership_lost
          | :timeout
          | :unknown_outcome
          | {:revision_mismatch, non_neg_integer()}
          | :space_exceeded
          | :invalid_key
          | :key_too_large
          | :value_too_large
          | :invalid_limit
          | :invalid_limits
          | :malformed_command
          | :session_expired
          | :stale_command
          | :too_many_pending
          | :overloaded
          | :not_ready
          | :shutting_down
          | :transfer_in_progress
          | :invalid_target

  @retryable ~w[
    no_leader
    leadership_lost
    timeout
    shutting_down
    overloaded
    not_ready
    transfer_in_progress
  ]a

  @spec indefinite?(reason()) :: boolean()
  def indefinite?(:leadership_lost), do: true
  def indefinite?(:timeout), do: true
  def indefinite?(:unknown_outcome), do: true
  def indefinite?(_reason), do: false

  @spec retryable?(reason()) :: boolean()
  def retryable?({:not_leader, _hint}), do: true
  def retryable?(reason), do: reason in @retryable
end
