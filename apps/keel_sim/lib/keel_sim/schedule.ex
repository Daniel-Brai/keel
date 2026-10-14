defmodule KeelSim.Schedule do
  @moduledoc "Load only files that you saved yourself"

  alias KeelSim.Cluster
  alias KeelSim.Swarm

  @type saved :: %{config: Swarm.config(), schedule: [Cluster.action()]}

  @spec save(Path.t(), Swarm.config(), [Cluster.action()]) :: :ok
  def save(path, config, schedule) do
    bytes = :erlang.term_to_binary(%{config: config, schedule: schedule})

    File.write!(path, bytes)
  end

  @spec load(Path.t()) :: {:ok, saved()} | {:error, :unreadable}
  def load(path) do
    with {:ok, bytes} <- File.read(path),
         %{config: _config, schedule: _schedule} = saved <- decode(bytes) do
      {:ok, saved}
    else
      _other -> {:error, :unreadable}
    end
  end

  defp decode(bytes) do
    :erlang.binary_to_term(bytes)
  rescue
    ArgumentError -> :error
  end
end
