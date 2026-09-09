defmodule Keel.Raft.Message.AppendEntries do
  alias Keel.Common.T

  @fields ~w[term from to prev_index prev_term entries commit seq]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id(),
          prev_index: T.index(),
          prev_term: T.term_number(),
          entries: [term()],
          commit: T.index(),
          seq: pos_integer()
        }
end
