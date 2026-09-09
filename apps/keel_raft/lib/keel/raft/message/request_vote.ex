defmodule Keel.Raft.Message.RequestVote do
  alias Keel.Common.T

  @required ~w[term from to last_log_index last_log_term]a
  @enforce_keys @required
  defstruct Enum.concat(@required, pre_vote?: false, transfer?: false)

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id(),
          last_log_index: T.index(),
          last_log_term: T.term_number(),
          pre_vote?: boolean(),
          transfer?: boolean()
        }
end
