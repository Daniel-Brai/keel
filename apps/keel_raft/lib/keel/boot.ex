defmodule Keel.Boot do
  @moduledoc "A configured group that fails to start stops the boot so a bad setting never runs half started"

  require Logger

  @type group :: {atom(), keyword()}

  @type failure :: {:group_failed, atom(), term()}

  @spec start_groups([group()]) :: :ok | {:error, failure()}
  def start_groups(groups), do: Enum.reduce_while(groups, :ok, &start_group/2)

  defp start_group({group, opts}, :ok) do
    started = Keel.start_group(group, opts)

    case started do
      {:ok, _pid} -> started_group(group)
      {:error, reason} -> {:halt, {:error, {:group_failed, group, reason}}}
    end
  end

  defp started_group(group) do
    Logger.info("started group #{group}")

    {:cont, :ok}
  end
end
