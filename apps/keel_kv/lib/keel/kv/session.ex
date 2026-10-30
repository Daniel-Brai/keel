defmodule Keel.KV.Session do
  @moduledoc "Time here is the log time of the entry and never a clock"

  alias Keel.KV.Command
  alias Keel.KV.Limits
  alias Keel.KV.Result

  @fields ~w[id results lowest_seq last_seen_ms]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          results: :gb_trees.tree(non_neg_integer(), Result.t()),
          lowest_seq: non_neg_integer(),
          last_seen_ms: non_neg_integer()
        }

  @type sessions :: :gb_trees.tree(non_neg_integer(), t())

  @type verdict ::
          {:ok, t()}
          | {:duplicate, Result.t()}
          | {:error, :session_expired | :stale_command | :too_many_pending}

  @spec new(non_neg_integer(), non_neg_integer()) :: t()
  def new(id, time_ms) do
    %__MODULE__{id: id, results: :gb_trees.empty(), lowest_seq: 1, last_seen_ms: time_ms}
  end

  @spec admit(sessions(), Command.t(), non_neg_integer(), Limits.t()) :: verdict()
  def admit(sessions, %Command{} = command, time_ms, limits) do
    case :gb_trees.lookup(command.client_id, sessions) do
      :none -> {:error, :session_expired}
      {:value, session} -> admit_seq(session, command, time_ms, limits)
    end
  end

  @spec record(t(), non_neg_integer(), Result.t()) :: t()
  def record(%__MODULE__{} = session, seq, result),
    do: %{session | results: :gb_trees.enter(seq, result, session.results)}

  @spec idle(sessions(), non_neg_integer(), non_neg_integer()) :: [non_neg_integer()]
  def idle(sessions, time_ms, ttl_ms) do
    for {id, session} <- :gb_trees.to_list(sessions), session.last_seen_ms + ttl_ms < time_ms do
      id
    end
  end

  @doc "Ties go to the lower id."
  @spec oldest(sessions()) :: non_neg_integer()
  def oldest(sessions) do
    {id, _session} =
      sessions
      |> :gb_trees.to_list()
      |> Enum.min_by(fn {id, session} -> {session.last_seen_ms, id} end)

    id
  end

  defp admit_seq(session, command, time_ms, limits) do
    cond do
      :gb_trees.is_defined(command.seq, session.results) ->
        {:duplicate, :gb_trees.get(command.seq, session.results)}

      command.seq < session.lowest_seq ->
        {:error, :stale_command}

      true ->
        acknowledge(session, command, time_ms, limits)
    end
  end

  defp acknowledge(session, command, time_ms, limits) do
    pruned = prune(session, min(command.ack, command.seq))

    if :gb_trees.size(pruned.results) >= limits.session_max_pending,
      do: {:error, :too_many_pending},
      else: {:ok, %{pruned | last_seen_ms: time_ms}}
  end

  # Acks only move forward so only the smallest results can go. Each write costs one look at the smallest
  defp prune(session, ack) do
    results = drop_below(session.results, ack)

    %{session | results: results, lowest_seq: max(session.lowest_seq, ack)}
  end

  defp drop_below(results, ack) do
    case :gb_trees.is_empty(results) do
      true -> results
      false -> drop_smallest(results, ack)
    end
  end

  defp drop_smallest(results, ack) do
    {seq, _result} = :gb_trees.smallest(results)

    case seq < ack do
      true -> drop_seq(results, seq, ack)
      false -> results
    end
  end

  defp drop_seq(results, seq, ack) do
    rest = :gb_trees.delete(seq, results)

    drop_below(rest, ack)
  end
end
