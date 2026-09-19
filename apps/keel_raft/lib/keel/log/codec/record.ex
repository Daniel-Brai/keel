defmodule Keel.Log.Codec.Record do
  alias Keel.Common.T
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState

  @fields ~w[body seq size]a
  @enforce_keys @fields
  defstruct @fields

  @type body ::
          {:entries, [Entry.t(), ...]}
          | {:hard_state, HardState.t()}
          | {:truncate, T.index()}
          | {:compacted, T.index()}
          | {:sync_mark, pos_integer(), non_neg_integer(), non_neg_integer()}

  @type t :: %__MODULE__{
          body: body(),
          seq: pos_integer(),
          size: pos_integer()
        }
end
