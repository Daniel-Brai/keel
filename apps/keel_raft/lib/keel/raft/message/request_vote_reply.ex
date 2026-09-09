defmodule Keel.Raft.Message.RequestVoteReply do
  alias Keel.Common.T

  @required ~w[term from to granted?]a
  @enforce_keys @required
  defstruct Enum.concat(@required, pre_vote?: false)

  @type t :: %__MODULE__{
          term: T.term_number(),
          from: T.member_id(),
          to: T.member_id(),
          granted?: boolean(),
          pre_vote?: boolean()
        }
end
