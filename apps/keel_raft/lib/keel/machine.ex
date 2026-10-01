defmodule Keel.Machine do
  exports =
    case Mix.env() do
      :test -> [Stub]
      _env -> []
    end

  use Boundary, top_level?: true, deps: [Keel.Raft], exports: exports

  alias Keel.Raft.Entry

  @type state :: term()

  @type result :: {:ok, term()} | {:error, term()}

  @callback init(config :: map()) :: state()

  @callback apply(state(), Entry.t()) :: {state(), result()}

  @callback query(state(), query :: term()) :: result()

  @callback snapshot(state()) :: binary()

  @callback restore(binary()) :: {:ok, state()} | {:error, :corrupt_snapshot}
end
