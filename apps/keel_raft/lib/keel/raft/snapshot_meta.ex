defmodule Keel.Raft.SnapshotMeta do
  alias Keel.Common.T
  alias Keel.Raft.Config

  @fields ~w[index term time_ms size checksum]a
  @enforce_keys @fields
  defstruct [:index, :term, :time_ms, :size, :checksum, config: nil]

  @type t :: %__MODULE__{
          index: T.index(),
          term: T.term_number(),
          time_ms: non_neg_integer(),
          size: non_neg_integer(),
          checksum: non_neg_integer(),
          config: nil | Config.t()
        }
end
