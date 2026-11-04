defmodule KeelSim.Linearizable do
  alias KeelSim.Client.Op
  alias KeelSim.Linearizable.Register

  @max_states 1_000_000

  @type verdict ::
          :ok
          | {:error, {:not_linearizable, binary(), [Op.t()]}}
          | {:unknown, binary()}

  @doc "Option `:max_states` caps the states one key may explore."
  @spec check([Op.t()], keyword()) :: verdict()
  def check(ops, opts \\ []) do
    max_states = Keyword.get(opts, :max_states, @max_states)

    ops
    |> Enum.group_by(& &1.key)
    |> Enum.sort()
    |> Enum.find_value(:ok, &check_key(&1, max_states))
  end

  @spec check_key({binary(), [Op.t()]}, pos_integer()) ::
          nil | {:error, term()} | {:unknown, binary()}
  def check_key({key, ops}, max_states) do
    entries = entries(ops)
    context = %{seen: MapSet.new(), states: 0, max: max_states, best: {-1, []}}

    case search(entries, Register.init(), 0, 0, context) do
      {:found, _context} -> nil
      {:stuck, found} -> {:error, {:not_linearizable, key, window(found.best)}}
      {:limit, _context} -> {:unknown, key}
    end
  end

  defp entries(ops) do
    ops
    |> Enum.reject(&skipped?/1)
    |> Enum.sort_by(&{&1.invoked_at, &1.process})
    |> Enum.with_index()
    |> Enum.map(fn {op, id} -> {id, op.invoked_at, ends_at(op), op} end)
  end

  defp skipped?(%Op{status: :fail}), do: true
  defp skipped?(%Op{status: status, kind: :get}) when status != :ok, do: true
  defp skipped?(%Op{}), do: false

  defp ends_at(%Op{status: :ok, completed_at: at}), do: at
  defp ends_at(%Op{}), do: :infinity

  defp search(pending, state, done, depth, context) do
    if Enum.all?(pending, &optional?/1),
      do: {:found, context},
      else: expand(pending, state, done, depth, context)
  end

  defp expand(pending, state, done, depth, context) do
    key = {done, state}

    cond do
      MapSet.member?(context.seen, key) -> {:stuck, context}
      context.states >= context.max -> {:limit, context}
      true -> descend(pending, state, key, done, depth, context)
    end
  end

  defp descend(pending, state, key, done, depth, context) do
    candidates = minimal(pending)
    visited = visit(context, key, depth, candidates)

    try_each(candidates, pending, state, done, depth, visited)
  end

  defp visit(context, key, depth, candidates) do
    {best_depth, _window} = context.best
    best = if depth > best_depth, do: {depth, candidates}, else: context.best

    %{context | seen: MapSet.put(context.seen, key), states: context.states + 1, best: best}
  end

  defp try_each([], _pending, _state, _done, _depth, context), do: {:stuck, context}

  defp try_each([entry | rest], pending, state, done, depth, context) do
    {id, _invoked, _ends, op} = entry

    case Register.step(state, op) do
      :invalid ->
        try_each(rest, pending, state, done, depth, context)

      {:ok, next} ->
        remaining = List.keydelete(pending, id, 0)
        mask = Bitwise.bor(done, Bitwise.bsl(1, id))

        case search(remaining, next, mask, depth + 1, context) do
          {:stuck, grown} -> try_each(rest, pending, state, done, depth, grown)
          other -> other
        end
    end
  end

  # An operation can go first when no pending operation ended before it began
  defp minimal(pending) do
    earliest = pending |> Enum.map(&elem(&1, 2)) |> Enum.min()

    Enum.take_while(pending, fn {_id, invoked, _ends, _op} -> invoked <= earliest end)
  end

  defp optional?({_id, _invoked, ends, _op}), do: ends == :infinity

  defp window({_depth, candidates}), do: Enum.map(candidates, &elem(&1, 3))
end
