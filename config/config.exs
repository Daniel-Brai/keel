import Config

if config_env() == :dev do
  config :git_hooks,
    auto_install: true,
    verbose: true,
    hooks: [
      pre_commit: [
        tasks: [
          {:mix_task, :format, ["--check-formatted"]},
          {:mix_task, :credo, ["--strict"]}
        ]
      ],
      pre_push: [
        tasks: [
          {:cmd, "mix compile --warnings-as-errors --force"},
          {:cmd, "mix test --max-failures 1"}
        ]
      ]
    ]
end
