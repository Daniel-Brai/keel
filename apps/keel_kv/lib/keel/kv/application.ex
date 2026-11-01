defmodule Keel.KV.Application do
  use Application

  @impl Application
  def start(_type, _args) do
    children = [{DynamicSupervisor, name: Keel.KV.Writers, strategy: :one_for_one}]

    Supervisor.start_link(children, strategy: :one_for_one, name: Keel.KV.Supervisor)
  end
end
