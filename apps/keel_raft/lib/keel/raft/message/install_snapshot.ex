defmodule Keel.Raft.Message.InstallSnapshot do
  alias Keel.Common.T
  alias Keel.Raft.SnapshotMeta

  @fields ~w[term from to meta offset data done? seq]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id(),
          meta: SnapshotMeta.t(),
          offset: non_neg_integer(),
          data: binary(),
          done?: boolean(),
          seq: pos_integer()
        }
end
