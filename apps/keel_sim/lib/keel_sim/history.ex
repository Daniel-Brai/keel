defmodule KeelSim.History do
  alias KeelSim.Client.Op

  @type event ::
          {:invoke, non_neg_integer(), Op.t()}
          | {:ok | :fail | :info, non_neg_integer(), Op.t()}

  @spec ops([event()]) :: [Op.t()]
  def ops(events) do
    ended = for {status, _time, op} <- events, status != :invoke, do: op

    Enum.sort_by(ended, &{&1.invoked_at, &1.process})
  end
end
