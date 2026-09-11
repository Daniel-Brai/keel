defmodule Keel.Raft.Config do
  @moduledoc "A removed id stays in `removed` for good so it can never join again"

  alias Keel.Common.T
  alias Keel.Raft.Entry
  alias Keel.Raft.Log

  @enforce_keys ~w[voters learners removed]a
  defstruct voters: MapSet.new(), learners: MapSet.new(), removed: MapSet.new(), addresses: %{}

  @type t :: %__MODULE__{
          voters: MapSet.t(T.member_id()),
          learners: MapSet.t(T.member_id()),
          removed: MapSet.t(T.member_id()),
          addresses: %{T.member_id() => String.t()}
        }

  @type change ::
          {:add_learner, T.member_id()}
          | {:add_learner, T.member_id(), String.t()}
          | {:promote, T.member_id()}
          | {:remove, T.member_id()}

  @type refusal :: :already_member | :member_removed | :not_member | :last_voter

  @spec new([T.member_id()]) :: t()
  def new(voters) do
    %__MODULE__{voters: MapSet.new(voters), learners: MapSet.new(), removed: MapSet.new()}
  end

  @spec voters(t()) :: [T.member_id()]
  def voters(%__MODULE__{voters: voters}), do: Enum.sort(voters)

  @spec learners(t()) :: [T.member_id()]
  def learners(%__MODULE__{learners: learners}), do: Enum.sort(learners)

  @spec removed(t()) :: [T.member_id()]
  def removed(%__MODULE__{removed: removed}), do: Enum.sort(removed)

  @spec members(t()) :: [T.member_id()]
  def members(%__MODULE__{} = config) do
    config |> voters() |> Enum.concat(learners(config)) |> Enum.sort()
  end

  @spec voter?(t(), T.member_id()) :: boolean()
  def voter?(%__MODULE__{voters: voters}, id), do: MapSet.member?(voters, id)

  @spec learner?(t(), T.member_id()) :: boolean()
  def learner?(%__MODULE__{learners: learners}, id), do: MapSet.member?(learners, id)

  @spec member?(t(), T.member_id()) :: boolean()
  def member?(%__MODULE__{} = config, id), do: voter?(config, id) or learner?(config, id)

  @spec removed?(t(), T.member_id()) :: boolean()
  def removed?(%__MODULE__{removed: removed}, id), do: MapSet.member?(removed, id)

  @spec quorum(t()) :: pos_integer()
  def quorum(%__MODULE__{voters: voters}), do: div(MapSet.size(voters), 2) + 1

  @spec voting(t(), Enumerable.t()) :: non_neg_integer()
  def voting(%__MODULE__{} = config, ids), do: Enum.count(ids, &voter?(config, &1))

  @spec apply_change(t(), change()) :: {:ok, t()} | {:error, refusal()}
  def apply_change(%__MODULE__{} = config, {:add_learner, id}), do: add_learner(config, id, nil)

  def apply_change(%__MODULE__{} = config, {:add_learner, id, address}),
    do: add_learner(config, id, address)

  def apply_change(%__MODULE__{} = config, {:promote, id}), do: promote(config, id)
  def apply_change(%__MODULE__{} = config, {:remove, id}), do: remove(config, id)

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = config) do
    parts =
      {voters(config), learners(config), removed(config), sorted(config.addresses)}

    :erlang.term_to_binary(parts)
  end

  @spec decode(binary()) :: {:ok, t()} | :error
  def decode(binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      {voters, learners, removed, addresses} when is_list(addresses) ->
        {:ok, build(voters, learners, removed, addresses)}

      _other ->
        :error
    end
  rescue
    ArgumentError -> :error
  end

  @spec decode!(binary()) :: t()
  def decode!(binary) do
    {:ok, config} = decode(binary)

    config
  end

  @doc "The newest config entry in the log wins. A log without one has the config of its snapshot."
  @spec current(Log.t()) :: nil | t()
  def current(%Log{} = log) do
    case Log.last_config_entry(log) do
      nil -> held(log)
      %Entry{data: data} -> decode!(data)
    end
  end

  defp held(%Log{snapshot_config: nil, base_config: base}), do: base
  defp held(%Log{snapshot_config: config}), do: config

  defp build(voters, learners, removed, addresses) do
    %__MODULE__{
      voters: MapSet.new(voters),
      learners: MapSet.new(learners),
      removed: MapSet.new(removed),
      addresses: Map.new(addresses)
    }
  end

  defp sorted(map), do: Enum.sort(map)

  defp add_learner(config, id, address) do
    cond do
      member?(config, id) -> {:error, :already_member}
      removed?(config, id) -> {:error, :member_removed}
      true -> {:ok, learned(config, id, address)}
    end
  end

  defp learned(config, id, address) do
    learners = MapSet.put(config.learners, id)

    %{config | learners: learners, addresses: located(config.addresses, id, address)}
  end

  defp located(addresses, _id, nil), do: addresses
  defp located(addresses, id, address), do: Map.put(addresses, id, address)

  defp promote(config, id) do
    cond do
      learner?(config, id) -> {:ok, promoted(config, id)}
      voter?(config, id) -> {:error, :already_member}
      true -> {:error, :not_member}
    end
  end

  defp promoted(config, id) do
    learners = MapSet.delete(config.learners, id)

    %{config | learners: learners, voters: MapSet.put(config.voters, id)}
  end

  defp remove(config, id) do
    cond do
      not member?(config, id) -> {:error, :not_member}
      voter?(config, id) and MapSet.size(config.voters) == 1 -> {:error, :last_voter}
      true -> {:ok, dropped(config, id)}
    end
  end

  defp dropped(config, id) do
    %{
      config
      | voters: MapSet.delete(config.voters, id),
        learners: MapSet.delete(config.learners, id),
        removed: MapSet.put(config.removed, id),
        addresses: Map.delete(config.addresses, id)
    }
  end
end
