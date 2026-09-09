defmodule Keel.Raft.HardState do
  alias Keel.Common.T

  @fields ~w[term voted_for commit]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          term: T.term_number(),
          voted_for: nil | T.member_id(),
          commit: T.index()
        }
end
