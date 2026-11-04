defmodule KeelSim.Client.Op do
  @moduledoc "An `:info` operation may take effect at any time after it was invoked or never"

  @enforce_keys ~w[process kind key invoked_at]a
  defstruct [
    :process,
    :kind,
    :key,
    :invoked_at,
    :value,
    :expected,
    :completed_at,
    :result,
    status: :pending
  ]

  @type kind :: :put | :get | :delete | :compare_and_set

  @type status :: :pending | :ok | :fail | :info

  @type t :: %__MODULE__{
          process: pos_integer(),
          kind: kind(),
          key: binary(),
          invoked_at: non_neg_integer(),
          value: nil | binary(),
          expected: nil | non_neg_integer(),
          completed_at: nil | non_neg_integer(),
          result: term(),
          status: status()
        }
end
