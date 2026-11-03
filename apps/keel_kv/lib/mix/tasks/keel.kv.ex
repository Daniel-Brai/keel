defmodule Mix.Tasks.Keel.Kv do
  @shortdoc "Reads and writes keys of a Keel group"

  use Boundary, classify_to: Keel.KV
  use Mix.Task

  alias Keel.KV.CLI
  alias Mix.Tasks.Keel.Kv.Connection

  @impl Mix.Task
  def run(args) do
    case CLI.plan(args) do
      {:ok, plan} -> call(plan)
      {:error, message} -> Mix.raise(message)
    end
  end

  defp call(plan) do
    {kind, call_args} = plan.request
    :ok = Connection.start()
    opts = [timeout: plan.timeout]

    result =
      :erpc.call(
        plan.node,
        CLI,
        :execute,
        [plan.group, kind, call_args, opts],
        plan.timeout + 1_000
      )

    shell = Mix.shell()

    lines = CLI.render(result, plan.show?)

    Enum.each(lines, &shell.info/1)
  end
end
