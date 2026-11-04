defmodule KeelSim.FaultyKv do
  @moduledoc "Every member runs the same bug so only the history checks can see it"

  @behaviour Keel.Machine

  alias Keel.KV.Command
  alias Keel.KV.Machine
  alias Keel.Raft.Entry

  @enforce_keys [:bug, :kv]
  defstruct [:bug, :kv]

  @type t :: %__MODULE__{bug: :kv_drop_put | :kv_no_dedupe, kv: Machine.t()}

  @impl Keel.Machine
  def init(%{bug: bug} = config), do: %__MODULE__{bug: bug, kv: Machine.init(config)}

  @impl Keel.Machine
  def apply(%__MODULE__{bug: :kv_drop_put} = state, %Entry{} = entry) do
    case Command.decode(entry.data) do
      {:ok, %Command{op: {:put, _key, _value}}} when rem(entry.index, 7) == 0 ->
        {state, {:ok, entry.index}}

      _other ->
        delegate(state, entry)
    end
  end

  def apply(%__MODULE__{bug: :kv_no_dedupe} = state, %Entry{} = entry) do
    case Command.decode(entry.data) do
      {:ok, %Command{op: :register}} ->
        delegate(state, entry)

      {:ok, %Command{} = command} ->
        fresh = %Command{command | seq: entry.index, ack: entry.index}
        data = Command.encode(fresh)

        delegate(state, %Entry{entry | data: data})

      {:error, _reason} ->
        delegate(state, entry)
    end
  end

  @impl Keel.Machine
  def query(%__MODULE__{kv: kv}, query), do: Machine.query(kv, query)

  @impl Keel.Machine
  def snapshot(%__MODULE__{kv: kv}), do: Machine.snapshot(kv)

  @impl Keel.Machine
  def restore(bytes), do: restore(:kv_drop_put, bytes)

  @spec restore(:kv_drop_put | :kv_no_dedupe, binary()) ::
          {:ok, t()} | {:error, :corrupt_snapshot}
  def restore(bug, bytes) do
    with {:ok, kv} <- Machine.restore(bytes), do: {:ok, %__MODULE__{bug: bug, kv: kv}}
  end

  defp delegate(%__MODULE__{kv: kv} = state, entry) do
    {next, result} = Machine.apply(kv, entry)

    {%__MODULE__{state | kv: next}, result}
  end
end
