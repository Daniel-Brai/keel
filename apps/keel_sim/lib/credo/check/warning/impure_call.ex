if Code.ensure_loaded?(Credo.Check) do
  defmodule Credo.Check.Warning.ImpureCall do
    use Boundary, top_level?: true

    use Credo.Check,
      base_priority: :high,
      category: :warning,
      param_defaults: [
        pure: [
          "Keel.Raft",
          "Keel.Machine",
          "Keel.KV.Command",
          "Keel.KV.Limits",
          "Keel.KV.Machine",
          "Keel.KV.Result",
          "Keel.KV.Session",
          "Keel.KV.Snapshot",
          "Keel.KV.State",
          "Keel.Chain.Machine",
          "Keel.Log.Codec",
          "Keel.Log.Recovery"
        ]
      ],
      explanations: [
        check: """
        The core and the state machines are pure. They take a state and an
        event. They return a new state and effects. They never touch time,
        global randomness, processes, messages, shared tables, IO, logs or
        config.

        Time enters as a tick or a field on an event. Randomness is a `:rand`
        state kept in the struct. Only the `_s` functions use it.
        """,
        params: [pure: "Module name prefixes that must stay pure."]
      ]

    @banned_modules ~w[Process GenServer Task Agent File IO Logger Port]
    @typespecs ~w[type typep opaque spec callback]a
    @banned_erlang ~w[ets persistent_term counters atomics file prim_file telemetry logger]a
    @banned_local ~w[send spawn spawn_link spawn_monitor make_ref self receive]a
    @allowed_rand ~w[seed_s uniform_s uniform_real_s normal_s]a

    @impl Credo.Check
    def run(%SourceFile{} = source_file, params) do
      ctx = Context.build(source_file, params, __MODULE__)
      pure = Params.get(params, :pure, __MODULE__)
      result = Credo.Code.prewalk(source_file, &walk(&1, &2, pure), ctx)

      result.issues
    end

    defp walk({:defmodule, _meta, [{:__aliases__, _alias_meta, parts} | _body]} = ast, ctx, pure) do
      name = Enum.map_join(parts, ".", &to_string/1)

      if pure?(name, pure) do
        {nil, Credo.Code.prewalk(ast, &banned/2, ctx)}
      else
        {ast, ctx}
      end
    end

    defp walk(ast, ctx, _pure), do: {ast, ctx}

    defp pure?(name, pure) do
      not String.ends_with?(name, "Test") and
        Enum.any?(pure, &(name == &1 or String.starts_with?(name, &1 <> ".")))
    end

    # Types name modules such as :rand.state() without calling them
    defp banned({:@, _meta, [{attribute, _attr_meta, _type}]}, ctx) when attribute in @typespecs,
      do: {nil, ctx}

    defp banned({{:., _dot, [{:__aliases__, _alias, parts}, fun]}, meta, _args} = ast, ctx) do
      module = Enum.map_join(parts, ".", &to_string/1)
      banned? = banned_elixir?(module, fun)

      {ast, maybe_issue(ctx, banned?, meta, "#{module}.#{fun}")}
    end

    defp banned({{:., _dot, [module, fun]}, meta, _args} = ast, ctx) when is_atom(module) do
      banned? = banned_erlang?(module, fun)

      {ast, maybe_issue(ctx, banned?, meta, ":#{module}.#{fun}")}
    end

    defp banned({fun, meta, args} = ast, ctx) when fun in @banned_local and is_list(args) do
      {ast, maybe_issue(ctx, true, meta, to_string(fun))}
    end

    defp banned({:node, meta, []} = ast, ctx), do: {ast, maybe_issue(ctx, true, meta, "node")}
    defp banned(ast, ctx), do: {ast, ctx}

    defp banned_elixir?(module, _fun) when module in @banned_modules, do: true
    defp banned_elixir?("System", fun), do: fun |> to_string() |> String.contains?("time")
    defp banned_elixir?(module, :utc_now) when module in ~w[DateTime NaiveDateTime], do: true
    defp banned_elixir?("Enum", fun) when fun in ~w[random shuffle take_random]a, do: true
    defp banned_elixir?("Application", _fun), do: true
    defp banned_elixir?(_module, _fun), do: false

    defp banned_erlang?(module, _fun) when module in @banned_erlang, do: true
    defp banned_erlang?(:rand, fun), do: fun not in @allowed_rand

    defp banned_erlang?(module, fun) when module in [:os, :erlang],
      do: time?(fun) or fun in ~w[send spawn self make_ref]a

    defp banned_erlang?(_module, _fun), do: false

    defp time?(fun), do: fun |> to_string() |> String.contains?("time")

    defp maybe_issue(ctx, false, _meta, _call), do: ctx

    defp maybe_issue(ctx, true, meta, call) do
      message = "`#{call}` is not allowed in a pure module. Return an effect or take an event."
      issue = format_issue(ctx, message: message, trigger: call, line_no: meta[:line])

      put_issue(ctx, issue)
    end
  end
end
