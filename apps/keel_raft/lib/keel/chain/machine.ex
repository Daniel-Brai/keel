defmodule Keel.Chain.Machine do
  @behaviour Keel.Machine

  alias Keel.Raft.Entry

  @fields [:count, :hash]
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{count: non_neg_integer(), hash: binary()}

  @impl Keel.Machine
  def init(_config), do: %__MODULE__{count: 0, hash: <<0::256>>}

  @impl Keel.Machine
  def apply(%__MODULE__{} = chain, %Entry{} = entry) do
    block = [chain.hash, <<entry.index::64, entry.term::64>>, entry.data]
    next = %__MODULE__{count: chain.count + 1, hash: :crypto.hash(:sha256, block)}

    {next, {:ok, {next.count, next.hash}}}
  end

  @impl Keel.Machine
  def query(%__MODULE__{} = chain, :count), do: {:ok, chain.count}
  def query(%__MODULE__{}, _query), do: {:error, :unsupported}

  @impl Keel.Machine
  def snapshot(%__MODULE__{} = chain), do: <<chain.count::64, chain.hash::binary-size(32)>>

  @impl Keel.Machine
  def restore(bytes) do
    case bytes do
      <<count::64, hash::binary-size(32)>> -> {:ok, %__MODULE__{count: count, hash: hash}}
      _other -> {:error, :corrupt_snapshot}
    end
  end
end
