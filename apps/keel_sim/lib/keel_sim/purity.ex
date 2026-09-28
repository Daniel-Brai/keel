defmodule KeelSim.Purity do
  @moduledoc "The Credo check reads one module so this one also sees calls through helpers elsewhere"

  @pure_prefixes ~w[
    Elixir.Keel.Raft
    Elixir.Keel.Machine
    Elixir.Keel.KV.Command
    Elixir.Keel.KV.Limits
    Elixir.Keel.KV.Machine
    Elixir.Keel.KV.Result
    Elixir.Keel.KV.Session
    Elixir.Keel.KV.Snapshot
    Elixir.Keel.KV.State
    Elixir.Keel.Chain.Machine
    Elixir.Keel.Log.Codec
    Elixir.Keel.Log.Recovery
  ]

  @pure_library [
    Access,
    ArgumentError,
    Bitwise,
    Enum,
    Integer,
    Keyword,
    Kernel,
    List,
    Map,
    MapSet,
    Range,
    String.Chars,
    String,
    Tuple,
    :binary,
    :elixir_erl_pass,
    :gb_trees,
    :lists,
    :maps,
    :queue
  ]

  @banned_erlang ~w[send spawn spawn_link spawn_monitor self make_ref node put get erase]a
  @allowed_rand ~w[seed_s uniform_s uniform_real_s normal_s]a

  @type call :: {module(), atom(), arity()}

  @type violation :: {module(), call()}

  @spec pure?(module()) :: boolean()
  def pure?(module) do
    name = Atom.to_string(module)
    Enum.any?(@pure_prefixes, &(name == &1 or String.starts_with?(name, &1 <> ".")))
  end

  @spec violations([{module(), [call()]}]) :: [violation()]
  def violations(modules) do
    modules
    |> Enum.filter(fn {module, _calls} -> pure?(module) end)
    |> Enum.flat_map(&module_violations/1)
    |> Enum.sort()
  end

  @spec imports(binary() | charlist()) :: {module(), [call()]}
  def imports(beam) do
    {:ok, {module, [imports: calls]}} = :beam_lib.chunks(beam, [:imports])
    {module, calls}
  end

  defp module_violations({module, calls}) do
    calls
    |> Enum.reject(&allowed?/1)
    |> Enum.map(&{module, &1})
  end

  defp allowed?({module, _fun, _arity}) when module in @pure_library, do: true
  defp allowed?({:erlang, fun, _arity}), do: fun not in @banned_erlang and not time?(fun)
  defp allowed?({:rand, fun, _arity}), do: fun in @allowed_rand
  defp allowed?({:crypto, fun, _arity}), do: fun == :hash
  defp allowed?({module, _fun, _arity}), do: pure?(module)

  defp time?(fun), do: fun |> Atom.to_string() |> String.contains?("time")
end
