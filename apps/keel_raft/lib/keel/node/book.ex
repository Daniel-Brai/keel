defmodule Keel.Node.Book do
  @moduledoc "The address book gains a member when its config entry is appended and loses it when its removal is applied"

  alias Keel.Common.String, as: Atoms
  alias Keel.Raft.Config
  alias Keel.Transport

  require Logger

  @spec follow(Transport.members(), Config.t()) :: Transport.members()
  def follow(members, %Config{} = config) do
    config
    |> Config.members()
    |> Enum.reject(&Map.has_key?(members, &1))
    |> Enum.reduce(members, &learn(&2, config, &1))
  end

  @spec forget(Transport.members(), Config.t()) :: Transport.members()
  def forget(members, %Config{removed: removed}), do: Map.drop(members, MapSet.to_list(removed))

  defp learn(members, config, id) do
    case Map.fetch(config.addresses, id) do
      {:ok, name} -> locate(members, id, name)
      :error -> members
    end
  end

  defp locate(members, id, name) do
    node = Atoms.to_atom(name)

    Map.put(members, id, node)
  rescue
    ArgumentError ->
      Logger.warning("member #{id} is at #{name} which this node has never heard of")

      members
  end
end
