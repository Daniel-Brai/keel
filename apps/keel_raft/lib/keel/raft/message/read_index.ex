defmodule Keel.Raft.Message.ReadIndex do
  alias Keel.Common.T

  @fields ~w[term from to ref_id]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id(),
          ref_id: pos_integer()
        }
end
