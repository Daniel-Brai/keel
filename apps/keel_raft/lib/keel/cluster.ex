defmodule Keel.Cluster do
  use Boundary, top_level?: true, deps: [Keel, Keel.Node], exports: []

  alias Keel.Node.Server

  @status_timeout_ms 2_000

  @type member_status :: {:ok, Server.status()} | {:error, term()}

  @spec status(atom(), keyword()) :: %{String.t() => member_status()}
  def status(group, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @status_timeout_ms)
    local = Keel.status(group)
    members = Enum.sort(local.members)
    nodes = Enum.map(members, fn {_id, node} -> node end)
    replies = :erpc.multicall(nodes, Keel, :status, [group], timeout)

    members
    |> Enum.zip(replies)
    |> Map.new(fn {{id, _node}, reply} -> {id, reply(reply)} end)
  end

  defp reply({:ok, status}), do: {:ok, status}
  defp reply({:error, reason}), do: {:error, reason}
  defp reply({class, reason}), do: {:error, {class, reason}}
end
