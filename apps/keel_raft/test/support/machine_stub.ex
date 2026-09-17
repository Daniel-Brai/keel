defmodule Keel.Machine.Stub do
  @behaviour Keel.Machine

  alias Keel.Raft.Entry

  @impl Keel.Machine
  def init(_config), do: 0

  @impl Keel.Machine
  def apply(_count, %Entry{data: "crash"} = entry), do: String.to_integer(entry.data)
  def apply(count, %Entry{}), do: {count + 1, {:ok, count + 1}}

  @impl Keel.Machine
  def query(count, :count), do: {:ok, count}
  def query(_count, _query), do: {:error, :unsupported}

  @impl Keel.Machine
  def snapshot(count), do: <<count::64>>

  @impl Keel.Machine
  def restore(<<count::64>>), do: {:ok, count}
  def restore(_bytes), do: {:error, :corrupt_snapshot}
end
