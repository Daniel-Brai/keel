defmodule Keel.KV.Machine do
  @moduledoc "A sweep runs at a fixed applied index so every member expires the same sessions at the same entry"

  @behaviour Keel.Machine

  alias Keel.KV.Command
  alias Keel.KV.Limits
  alias Keel.KV.Session
  alias Keel.KV.Snapshot
  alias Keel.KV.State
  alias Keel.Raft.Entry

  @type t :: State.t()

  @impl Keel.Machine
  def init(config), do: config |> Limits.new() |> State.new()

  @impl Keel.Machine
  def apply(%State{} = state, %Entry{kind: :command} = entry) do
    applied =
      case Command.decode(entry.data) do
        {:ok, command} -> execute(state, command, entry)
        {:error, reason} -> {state, {:error, reason}}
      end

    sweep(applied, entry)
  end

  def apply(%State{} = state, %Entry{}), do: {state, {:ok, nil}}

  @impl Keel.Machine
  def query(%State{} = state, op) do
    if Command.read?(op) do
      {_state, result} = operate(state, op, nil)

      result
    else
      {:error, :unsupported}
    end
  end

  @impl Keel.Machine
  def snapshot(%State{} = state), do: Snapshot.encode(state)

  @impl Keel.Machine
  def restore(bytes), do: Snapshot.decode(bytes)

  @spec fetch(State.t(), Keel.KV.key()) :: nil | State.stored()
  def fetch(%State{} = state, key) do
    case :gb_trees.lookup(key, state.tree) do
      {:value, record} -> record
      :none -> nil
    end
  end

  defp execute(state, %Command{op: :register}, entry), do: register(state, entry)

  defp execute(state, %Command{} = command, entry) do
    case Session.admit(state.sessions, command, entry.time_ms, state.limits) do
      {:ok, session} -> run(state, session, command, entry)
      {:duplicate, result} -> {state, result}
      {:error, reason} -> {state, {:error, reason}}
    end
  end

  defp register(state, entry) do
    room = make_room(state)
    session = Session.new(entry.index, entry.time_ms)
    sessions = :gb_trees.insert(entry.index, session, room.sessions)

    {%{room | sessions: sessions}, {:ok, entry.index}}
  end

  defp make_room(state) do
    if :gb_trees.size(state.sessions) >= state.limits.max_sessions do
      oldest = Session.oldest(state.sessions)
      sessions = :gb_trees.delete(oldest, state.sessions)

      make_room(%{state | sessions: sessions})
    else
      state
    end
  end

  defp run(state, session, %Command{op: op} = command, entry) do
    {next, result} = operate(state, op, entry)
    kept = if Command.read?(op), do: session, else: Session.record(session, command.seq, result)
    sessions = :gb_trees.enter(session.id, kept, next.sessions)

    {%{next | sessions: sessions}, result}
  end

  defp operate(state, {:put, key, value}, entry) do
    with :ok <- check_key(state, key),
         :ok <- check_value(state, value),
         {:ok, next} <- store(state, key, value, entry.index) do
      {next, {:ok, entry.index}}
    else
      {:error, reason} -> {state, {:error, reason}}
    end
  end

  defp operate(state, {:delete, key}, entry) do
    case :gb_trees.lookup(key, state.tree) do
      :none -> {state, {:ok, 0}}
      {:value, record} -> {remove(state, key, record, entry.index), {:ok, entry.index}}
    end
  end

  defp operate(state, {:compare_and_set, key, expected, change}, entry) do
    case check_change(state, key, change) do
      :ok -> compare_and_set(state, key, expected, change, entry)
      {:error, reason} -> {state, {:error, reason}}
    end
  end

  defp operate(state, {:set_limits, changes}, entry) do
    limits = Map.merge(state.limits, changes)

    if Limits.valid?(limits) and limits.max_data_bytes >= state.data_bytes,
      do: {%{state | limits: limits}, {:ok, entry.index}},
      else: {state, {:error, :invalid_limits}}
  end

  defp operate(state, {:get, key}, _entry) do
    case fetch(state, key) do
      nil -> {state, {:ok, nil}}
      record -> {state, {:ok, record}}
    end
  end

  defp operate(state, {:range, from, to, limit}, _entry),
    do: scan(state, {:range, to}, :gb_trees.iterator_from(from, state.tree), limit)

  defp operate(state, {:prefix, prefix, after_key, limit}, _entry) do
    start = if after_key != nil and after_key >= prefix, do: after_key, else: prefix
    iterator = :gb_trees.iterator_from(start, state.tree)
    skipped = if start == after_key, do: skip_key(iterator, after_key), else: iterator

    scan(state, {:prefix, prefix}, skipped, limit)
  end

  defp compare_and_set(state, key, expected, change, entry) do
    current = revision_of(state, key)

    case {current == expected, change} do
      {false, _change} -> {state, {:error, {:revision_mismatch, current}}}
      {true, :delete} when current == 0 -> {state, {:error, {:revision_mismatch, 0}}}
      {true, :delete} -> {remove(state, key, fetch(state, key), entry.index), {:ok, entry.index}}
      {true, {:put, value}} -> compare_and_set_put(state, key, value, entry)
    end
  end

  defp compare_and_set_put(state, key, value, entry) do
    case store(state, key, value, entry.index) do
      {:ok, next} -> {next, {:ok, entry.index}}
      {:error, reason} -> {state, {:error, reason}}
    end
  end

  defp check_change(state, key, :delete), do: check_key(state, key)

  defp check_change(state, key, {:put, value}) do
    with :ok <- check_key(state, key), do: check_value(state, value)
  end

  defp check_key(_state, <<>>), do: {:error, :invalid_key}

  defp check_key(state, key) do
    if byte_size(key) > state.limits.max_key_bytes, do: {:error, :key_too_large}, else: :ok
  end

  defp check_value(state, value) do
    if byte_size(value) > state.limits.max_value_bytes, do: {:error, :value_too_large}, else: :ok
  end

  defp revision_of(state, key) do
    case fetch(state, key) do
      nil -> 0
      {_value, revision} -> revision
    end
  end

  defp store(state, key, value, index) do
    old = bytes_held(state, key)
    data_bytes = state.data_bytes - old + State.record_bytes(key, value)

    if data_bytes > state.limits.max_data_bytes do
      {:error, :space_exceeded}
    else
      tree = :gb_trees.enter(key, {value, index}, state.tree)

      {:ok, %{state | tree: tree, data_bytes: data_bytes, revision: index}}
    end
  end

  defp bytes_held(state, key) do
    case fetch(state, key) do
      nil -> 0
      {value, _revision} -> State.record_bytes(key, value)
    end
  end

  defp remove(state, key, {value, _revision}, index) do
    tree = :gb_trees.delete(key, state.tree)
    data_bytes = state.data_bytes - State.record_bytes(key, value)

    %{state | tree: tree, data_bytes: data_bytes, revision: index}
  end

  defp scan(state, _within, _iterator, limit) when limit < 1,
    do: {state, {:error, :invalid_limit}}

  defp scan(state, within, iterator, limit) do
    capped = min(limit, state.limits.max_range_limit)
    page = collect(iterator, within, capped, [])

    {state, {:ok, page}}
  end

  defp collect(iterator, within, remaining, acc) do
    case :gb_trees.next(iterator) do
      :none ->
        {Enum.reverse(acc), nil}

      {key, {value, revision}, rest} ->
        cond do
          not inside?(within, key) -> {Enum.reverse(acc), nil}
          remaining == 0 -> {Enum.reverse(acc), key}
          true -> collect(rest, within, remaining - 1, [{key, value, revision} | acc])
        end
    end
  end

  defp inside?({:range, nil}, _key), do: true
  defp inside?({:range, to}, key), do: key < to

  defp inside?({:prefix, prefix}, key) do
    size = byte_size(prefix)

    byte_size(key) >= size and binary_part(key, 0, size) == prefix
  end

  defp skip_key(iterator, key) do
    case :gb_trees.next(iterator) do
      {^key, _record, rest} -> rest
      _other -> iterator
    end
  end

  defp sweep({%State{next_sweep: next} = state, result}, %Entry{index: index} = entry)
       when index >= next do
    idle = Session.idle(state.sessions, entry.time_ms, state.limits.session_ttl_ms)
    sessions = Enum.reduce(idle, state.sessions, &:gb_trees.delete(&1, &2))
    interval = state.limits.session_sweep_interval

    {%{state | sessions: sessions, next_sweep: index + interval}, result}
  end

  defp sweep({state, result}, _entry), do: {state, result}
end
