defmodule KeelSim.KvCheck do
  @moduledoc "Key prefix `p/` means one writer. `s/` means every client. `r/` means a compare and set round"

  alias KeelSim.Client.Op
  alias KeelSim.Linearizable

  @type violation ::
          {:garbage_read, binary()}
          | {:lost_write, binary()}
          | {:stale_read, binary()}
          | {:double_compare_and_set_win, binary()}
          | {:not_linearizable, binary()}
          | {:linearizability_unknown, binary()}

  @spec value(non_neg_integer(), non_neg_integer()) :: binary()
  def value(slot, counter) do
    payload = <<"v", slot::16, counter::32>>

    <<:erlang.crc32(payload)::32, payload::binary>>
  end

  @spec valid_value?(binary()) :: boolean()
  def valid_value?(value) do
    case value do
      <<crc::32, payload::binary>> -> :erlang.crc32(payload) == crc
      _other -> false
    end
  end

  @spec check([Op.t()], keyword()) :: [violation()]
  def check(ops, opts \\ []) do
    by_key = ops |> Enum.group_by(& &1.key) |> Enum.sort()

    cheap = Enum.flat_map(by_key, fn {key, key_ops} -> key_violations(key, key_ops) end)
    searched = linearizable(ops, opts)

    cheap |> Enum.concat(searched) |> Enum.uniq() |> Enum.sort()
  end

  defp key_violations(key, ops) do
    garbage = if garbage?(ops), do: [{:garbage_read, key}], else: []
    stale = if stale?(ops), do: [{:stale_read, key}], else: []
    lost = if lost?(key, ops), do: [{:lost_write, key}], else: []
    double = if double_win?(key, ops), do: [{:double_compare_and_set_win, key}], else: []

    garbage ++ stale ++ lost ++ double
  end

  defp linearizable(ops, opts) do
    case Linearizable.check(ops, opts) do
      :ok -> []
      {:error, {:not_linearizable, key, _window}} -> [{:not_linearizable, key}]
      {:unknown, key} -> [{:linearizability_unknown, key}]
    end
  end

  defp garbage?(ops) do
    written = for op <- ops, stored?(op), into: MapSet.new(), do: op.value
    seen = for %Op{kind: :get, status: :ok, result: {:ok, {value, _rev}}} <- ops, do: value

    Enum.any?(seen, fn value -> not valid_value?(value) or not MapSet.member?(written, value) end)
  end

  # A write that may have stored its value
  defp stored?(%Op{kind: kind, status: :info}), do: kind in [:put, :compare_and_set]
  defp stored?(%Op{kind: :put, status: :ok}), do: true
  defp stored?(%Op{kind: :compare_and_set, status: :ok, result: {:ok, _rev}}), do: true
  defp stored?(%Op{}), do: false

  defp stale?(ops) do
    writes =
      for %Op{status: :ok, completed_at: at, result: {:ok, rev}} = op <- ops,
          writing?(op),
          rev > 0,
          do: {at, rev, op}

    reads = for %Op{kind: :get, status: :ok} = op <- ops, do: op

    Enum.any?(reads, &behind?(&1, writes, ops))
  end

  defp writing?(%Op{kind: kind}), do: kind in ~w[put compare_and_set delete]a

  defp behind?(read, writes, ops) do
    before = for {at, rev, op} <- writes, at < read.invoked_at, do: {rev, op}

    case Enum.max_by(before, &elem(&1, 0), fn -> nil end) do
      nil -> false
      {rev, _write} -> older?(read, rev, ops)
    end
  end

  # An empty read is fine when a delete could have taken effect before the read ended
  defp older?(%Op{result: {:ok, nil}} = read, _rev, ops),
    do: not Enum.any?(ops, &deleted_before?(&1, read.completed_at))

  defp older?(%Op{result: {:ok, {_value, seen}}}, rev, _ops), do: seen < rev

  defp deleted_before?(%Op{kind: :delete, status: status, invoked_at: at}, ended),
    do: status != :fail and at < ended

  defp deleted_before?(%Op{}, _ended), do: false

  defp lost?("p/" <> _rest, ops) do
    ordered = Enum.sort_by(ops, &{&1.invoked_at, &1.process})
    outcome = Enum.reduce_while(ordered, MapSet.new([:absent]), &allow/2)

    outcome == :lost
  end

  defp lost?(_key, _ops), do: false

  defp allow(%Op{status: :fail}, allowed), do: {:cont, allowed}

  defp allow(%Op{kind: :get, status: :ok, result: {:ok, seen}}, allowed) do
    held = if seen == nil, do: :absent, else: elem(seen, 0)

    if MapSet.member?(allowed, held), do: {:cont, allowed}, else: {:halt, :lost}
  end

  defp allow(%Op{kind: :get}, allowed), do: {:cont, allowed}

  defp allow(%Op{kind: :put, status: :ok, value: value}, _allowed),
    do: {:cont, MapSet.new([value])}

  defp allow(
         %Op{kind: :compare_and_set, status: :ok, value: value, result: {:ok, _rev}},
         _allowed
       ),
       do: {:cont, MapSet.new([value])}

  defp allow(%Op{kind: :compare_and_set, status: :ok}, allowed), do: {:cont, allowed}
  defp allow(%Op{kind: :delete, status: :ok}, _allowed), do: {:cont, MapSet.new([:absent])}

  defp allow(%Op{kind: :delete, status: :info}, allowed),
    do: {:cont, MapSet.put(allowed, :absent)}

  defp allow(%Op{status: :info, value: value}, allowed), do: {:cont, MapSet.put(allowed, value)}
  defp allow(%Op{}, allowed), do: {:cont, allowed}

  defp double_win?("r/" <> _rest, ops) do
    wins =
      Enum.count(ops, &match?(%Op{kind: :compare_and_set, status: :ok, result: {:ok, _rev}}, &1))

    wins > 1
  end

  defp double_win?(_key, _ops), do: false
end
