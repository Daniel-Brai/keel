defmodule Keel.Log.Recovery.Result do
  alias Keel.Common.T
  alias Keel.Log.Codec.Record
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.SnapshotMeta

  @fields ~w[hard_state entries last_segment next_seq tail dropped torn_tail?]a
  @enforce_keys @fields
  defstruct @fields ++ [first_index: 1, snapshot: nil, keep_from: 1, segments: []]

  @type tail_record :: {pos_integer(), Record.body()}

  @type t :: %__MODULE__{
          hard_state: HardState.t(),
          entries: [Entry.t()],
          last_segment: nil | pos_integer(),
          next_seq: pos_integer(),
          tail: [tail_record()],
          dropped: [pos_integer()],
          torn_tail?: boolean(),
          first_index: T.index(),
          snapshot: nil | SnapshotMeta.t(),
          keep_from: T.index(),
          segments: [{pos_integer(), T.index()}]
        }
end
