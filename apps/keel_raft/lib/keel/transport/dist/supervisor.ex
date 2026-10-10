defmodule Keel.Transport.Dist.Supervisor do
  use Supervisor

  alias Keel.Common.Name

  @type group :: Keel.Transport.group()

  @spec registry(group()) :: atom()
  def registry(group), do: Name.build("keel", group, "_peers")

  @spec peers(group()) :: atom()
  def peers(group), do: Name.build("keel", group, "_senders")

  @spec settings(group()) :: keyword()
  def settings(group) do
    :persistent_term.get({__MODULE__, group})
  end

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl Supervisor
  def init(opts) do
    group = Keyword.fetch!(opts, :group)
    keys = ~w[cluster_id destination max_queue check_every]a
    :persistent_term.put({__MODULE__, group}, Keyword.take(opts, keys))

    children = [
      {Registry, keys: :unique, name: registry(group)},
      {DynamicSupervisor, name: peers(group), strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
