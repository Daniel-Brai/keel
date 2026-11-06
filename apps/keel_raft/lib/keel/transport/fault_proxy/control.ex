defmodule Keel.Transport.FaultProxy.Control do
  use GenServer

  alias Keel.Common.Name

  @type group :: Keel.Transport.group()

  @spec table(group()) :: atom()
  def table(group), do: Name.build("keel", group, "_faults")

  def start_link(opts) do
    group = Keyword.fetch!(opts, :group)

    GenServer.start_link(__MODULE__, opts, name: name(group))
  end

  @spec delay(group(), String.t(), term(), pos_integer(), non_neg_integer()) :: :ok
  def delay(group, to, message, copies, ms) do
    control = name(group)

    GenServer.cast(control, {:delay, to, message, copies, ms})
  end

  @impl GenServer
  def init(opts) do
    group = Keyword.fetch!(opts, :group)
    inner = Keyword.fetch!(opts, :inner)
    name = table(group)
    table = :ets.new(name, [:named_table, :public, :set, read_concurrency: true])

    true = :ets.insert(table, {:inner, inner})

    {:ok, %{group: group, inner: inner}}
  end

  @impl GenServer
  def handle_cast({:delay, to, message, copies, ms}, state) do
    Process.send_after(self(), {:release, to, message, copies}, ms)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:release, to, message, copies}, state) do
    Enum.each(1..copies, fn _copy -> state.inner.send(state.group, to, message) end)

    {:noreply, state}
  end

  defp name(group), do: Name.build("keel", group, "_fault_control")
end
