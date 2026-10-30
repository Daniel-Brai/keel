defmodule Keel.KV.State do
  @moduledoc "Ordered trees make every member visit records and sessions in one fixed order"

  alias Keel.KV
  alias Keel.KV.Limits
  alias Keel.KV.Session

  @record_overhead 80

  @fields ~w[tree sessions revision data_bytes next_sweep limits]a
  @enforce_keys @fields
  defstruct @fields

  @type stored :: {KV.value(), KV.revision()}

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(KV.key(), stored()),
          sessions: Session.sessions(),
          revision: KV.revision(),
          data_bytes: non_neg_integer(),
          next_sweep: non_neg_integer(),
          limits: Limits.t()
        }

  @spec new(Limits.t()) :: t()
  def new(limits) do
    %__MODULE__{
      tree: :gb_trees.empty(),
      sessions: :gb_trees.empty(),
      revision: 0,
      data_bytes: 0,
      next_sweep: limits.session_sweep_interval,
      limits: limits
    }
  end

  @spec record_bytes(KV.key(), KV.value()) :: non_neg_integer()
  def record_bytes(key, value), do: byte_size(key) + byte_size(value) + @record_overhead
end
