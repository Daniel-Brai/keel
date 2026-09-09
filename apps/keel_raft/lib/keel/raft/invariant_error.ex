defmodule Keel.Raft.InvariantError do
  alias Keel.Common.T

  defexception ~w[member invariant index]a

  @type invariant :: :truncate_committed

  @type t :: %__MODULE__{
          member: T.member_id(),
          invariant: invariant(),
          index: T.index()
        }

  @impl Exception
  def message(%__MODULE__{invariant: :truncate_committed} = error),
    do: "#{error.member} was asked to truncate committed entry #{error.index}"
end
