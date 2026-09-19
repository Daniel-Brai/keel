defmodule Keel.Log.Codec.Header do
  @fields ~w[segment cluster_id member_tag first_seq]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          segment: pos_integer(),
          cluster_id: <<_::128>>,
          member_tag: <<_::64>>,
          first_seq: pos_integer()
        }
end
