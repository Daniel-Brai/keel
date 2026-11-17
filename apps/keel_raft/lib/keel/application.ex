defmodule Keel.Application do
  use Application

  alias Keel.Boot

  # A release sets :groups from its environment in config/runtime.exs
  @impl Application
  def start(_type, _args) do
    children = [{DynamicSupervisor, name: Keel.Groups, strategy: :one_for_one}]
    supervisor_opts = [strategy: :one_for_one, name: Keel.Supervisor]
    groups = Application.get_env(:keel_raft, :groups, [])

    with {:ok, supervisor} <- Supervisor.start_link(children, supervisor_opts),
         :ok <- Boot.start_groups(groups) do
      {:ok, supervisor}
    end
  end
end
