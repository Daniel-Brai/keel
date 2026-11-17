defmodule Keel.KV.MixProject do
  use Mix.Project

  def project do
    [
      app: :keel_kv,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      compilers: [:boundary] ++ Mix.compilers(),
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto], mod: {Keel.KV.Application, []}]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:keel_raft, in_umbrella: true},
      {:boundary, "0.11.0", runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end
