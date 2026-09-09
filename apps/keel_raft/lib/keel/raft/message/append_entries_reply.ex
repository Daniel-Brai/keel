defmodule Keel.Raft.Message.AppendEntriesReply do
  alias Keel.Common.T

  @fields ~w[term from to success? match_index seq]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id(),
          success?: boolean(),
          match_index: T.index(),
          seq: pos_integer()
        }
end
