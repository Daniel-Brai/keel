defmodule Keel.Transport.Stub do
  @behaviour Keel.Transport

  alias Keel.Common.Name

  @impl Keel.Transport
  def child_spec(opts) do
    %{id: __MODULE__, start: {Agent, :start_link, [fn -> opts end]}}
  end

  @impl Keel.Transport
  def send(group, to, message) do
    sink = sink(group)

    Kernel.send(sink, {:sent, to, message})

    :ok
  end

  @impl Keel.Transport
  def update_members(_group, _members), do: :ok

  def listen(group), do: Process.register(self(), sink(group))

  defp sink(group), do: Name.build("keel_fake_sink", group)
end
