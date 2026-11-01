defmodule Keel.KV.Writer.Call do
  alias Keel.Error
  alias Keel.KV.Command
  alias Keel.KV.Outcome

  @fields ~w[
    id
    from
    op
    seq
    bytes
    attempt
    status
    maybe_applied?
    refusal
    redirects
    timer
    read?
  ]a
  @enforce_keys @fields
  defstruct @fields

  @type id :: pos_integer() | :register

  @type status :: :queued | :waiting | :backoff

  @type t :: %__MODULE__{
          id: id(),
          from: nil | GenServer.from(),
          op: :register | Command.op(),
          seq: nil | non_neg_integer(),
          bytes: nil | binary(),
          attempt: non_neg_integer(),
          status: status(),
          maybe_applied?: boolean(),
          refusal: nil | Error.reason(),
          redirects: non_neg_integer(),
          timer: nil | reference(),
          read?: boolean()
        }

  @spec new(id(), nil | GenServer.from(), :register | Command.op(), nil | reference()) :: t()
  def new(id, from, op, timer) do
    %__MODULE__{
      id: id,
      from: from,
      op: op,
      seq: nil,
      bytes: nil,
      attempt: 0,
      status: :queued,
      maybe_applied?: false,
      refusal: nil,
      redirects: 0,
      timer: timer,
      read?: false
    }
  end

  @spec retried(t(), Outcome.retry()) :: t()
  def retried(%__MODULE__{} = call, %{maybe_applied?: maybe?, refusal: refusal}) do
    kept = keep_refusal(refusal, call.refusal)

    %__MODULE__{
      call
      | maybe_applied?: call.maybe_applied? or (maybe? and not call.read?),
        refusal: kept,
        status: :backoff
    }
  end

  defp keep_refusal(nil, old), do: old
  defp keep_refusal(new, _old), do: new

  @spec deadline_error(t()) :: Error.reason()
  def deadline_error(%__MODULE__{maybe_applied?: true}), do: :unknown_outcome
  def deadline_error(%__MODULE__{refusal: nil}), do: :no_leader
  def deadline_error(%__MODULE__{refusal: refusal}), do: refusal
end
