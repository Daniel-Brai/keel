defmodule Keel.Raft.Message.InstallSnapshotReply do
  alias Keel.Common.T

  @fields ~w[term from to offset done? seq]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id(),
          offset: non_neg_integer(),
          done?: boolean(),
          seq: pos_integer()
        }
end
