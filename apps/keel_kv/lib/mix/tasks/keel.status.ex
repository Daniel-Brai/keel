defmodule Mix.Tasks.Keel.Status do
  @shortdoc "Prints the status of every member of a group"

  use Boundary, classify_to: Keel.KV
  use Mix.Task

  alias Keel.KV.CLI
  alias Mix.Tasks.Keel.Kv.Connection

  @impl Mix.Task
  def run(args) do
    case CLI.status_plan(args) do
      {:ok, plan} -> call(plan)
      {:error, message} -> Mix.raise(message)
    end
  end

  defp call(plan) do
    :ok = Connection.start()
    statuses = :erpc.call(plan.node, CLI, :status, [plan.group, []], plan.timeout)

    shell = Mix.shell()
    lines = for {id, status} <- Enum.sort(statuses), do: CLI.render_status(id, status)

    Enum.each(lines, &shell.info/1)
  end
end
