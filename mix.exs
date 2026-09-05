defmodule Keel.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "1.0.0",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:ex_unit, :mix, :credo], ignore_warnings: ".dialyzer_ignore.exs"],
      releases: [
        keel: [applications: [keel_raft: :permanent, keel_kv: :permanent]]
      ]
    ]
  end

  defp deps do
    [
      {:credo, "1.7.19", only: [:dev, :test], runtime: false},
      {:dialyxir, "1.4.8", only: [:dev, :test], runtime: false},
      {:git_hooks, "0.9.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "git_hooks.install"]
    ]
  end
end
