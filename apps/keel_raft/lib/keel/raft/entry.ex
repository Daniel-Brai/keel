defmodule Keel.Raft.Entry do
  alias Keel.Common.T

  @fields ~w[term index kind data time_ms]a
  @enforce_keys @fields
  defstruct @fields

  @overhead 29

  @type kind :: :command | :noop | :config

  @type t :: %__MODULE__{
          term: T.term_number(),
          index: T.index(),
          kind: kind(),
          data: binary(),
          time_ms: non_neg_integer()
        }

  @spec bytes(t()) :: non_neg_integer()
  def bytes(%__MODULE__{data: data}), do: @overhead + byte_size(data)
end
