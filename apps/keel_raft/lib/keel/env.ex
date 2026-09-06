defmodule Keel.Env do
  default = [Clock, Clock.System, Disk, Disk.Posix]

  exports =
    case Mix.env() do
      :test -> [Disk.Stub | default]
      _env -> default
    end

  use Boundary, top_level?: true, deps: [Keel.Common], exports: exports

  alias Keel.Env.Clock
  alias Keel.Env.Disk

  @fields ~w[clock disk seed]a
  @enforce_keys @fields
  defstruct @fields

  @type seed :: {integer(), integer(), integer()}

  @type t :: %__MODULE__{
          clock: module(),
          disk: Disk.t(),
          seed: seed()
        }

  @spec os(keyword()) :: t()
  def os(opts \\ []) do
    seed = opts |> Keyword.get(:seed) |> seed_or_random()

    %__MODULE__{
      clock: Clock.System,
      disk: Disk.Posix.new(),
      seed: seed
    }
  end

  defp seed_or_random(nil), do: random_seed()
  defp seed_or_random(seed), do: seed

  defp random_seed do
    <<a::32, b::32, c::32>> = :crypto.strong_rand_bytes(12)
    {a, b, c}
  end
end
