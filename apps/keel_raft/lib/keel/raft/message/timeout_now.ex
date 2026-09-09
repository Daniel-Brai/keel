defmodule Keel.Raft.Message.TimeoutNow do
  alias Keel.Common.T

  @fields ~w[term from to]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id()
        }
end
