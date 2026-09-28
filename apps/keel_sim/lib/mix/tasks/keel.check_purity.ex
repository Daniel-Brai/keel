defmodule Mix.Tasks.Keel.CheckPurity do
  @shortdoc "Fails if a pure module calls impure code in another module"

  use Mix.Task
  use Boundary, classify_to: KeelSim

  alias KeelSim.Purity

  @apps [:keel_raft, :keel_kv]

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("compile")

    @apps
    |> Enum.flat_map(&beams/1)
    |> Enum.map(&Purity.imports/1)
    |> Purity.violations()
    |> report()
  end

  defp beams(app) do
    dir = Application.app_dir(app, "ebin")
    pattern = Path.join(dir, "*.beam")

    pattern
    |> Path.wildcard()
    |> Enum.map(&String.to_charlist/1)
  end

  defp report([]), do: Mix.shell().info("Pure modules call only pure code")

  defp report(violations) do
    Enum.each(violations, fn {module, {to, fun, arity}} ->
      Mix.shell().error("#{inspect(module)} calls #{inspect(to)}.#{fun}/#{arity}")
    end)

    Mix.raise("#{length(violations)} impure calls from pure modules")
  end
end
