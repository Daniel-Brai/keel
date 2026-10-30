defmodule Keel.KV.MachineTest do
  use ExUnit.Case, async: true

  alias Keel.KV.Machine
  alias Keel.KV.TestUtils
  alias Keel.Raft.Entry

  @config %{
    max_key_bytes: 8,
    max_value_bytes: 16,
    max_data_bytes: 200,
    session_ttl_ms: 1_000,
    max_sessions: 3,
    session_max_pending: 2,
    session_sweep_interval: 10,
    max_range_limit: 3
  }

  @scan %{max_range_limit: 3}

  setup do
    machine = TestUtils.machine(@config)
    {state, client} = TestUtils.register(machine, 1, 0)

    %{state: state, client: client}
  end

  describe "query/2" do
    test "fails for a write", %{state: state} do
      result = Machine.query(state, {:put, "a", "1"})

      assert result == {:error, :unsupported}
    end

    test "fails for a scan with no room", %{state: state} do
      result = Machine.query(state, {:range, "a", nil, 0})

      assert result == {:error, :invalid_limit}
    end

    test "returns nil for a missing key", %{state: state} do
      result = Machine.query(state, {:get, "a"})

      assert result == {:ok, nil}
    end

    test "returns the record of a key", %{state: state, client: client} do
      {written, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})

      result = Machine.query(written, {:get, "a"})

      assert result == {:ok, {"1", 2}}
    end

    test "leaves the state alone", %{state: state, client: client} do
      {written, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      before = Machine.snapshot(written)

      _result = Machine.query(written, {:prefix, "a", nil, 3})

      assert Machine.snapshot(written) == before
    end
  end

  describe "apply/2" do
    test "fails for a client without a session", %{state: state} do
      {_state, result} = TestUtils.run(state, {2, 5}, {99, 1, 1}, {:put, "a", "1"})

      assert result == {:error, :session_expired}
    end

    test "fails on bytes that are not a command", %{state: state} do
      entry = TestUtils.entry(2, 5, "junk")

      applied = Machine.apply(state, entry)

      assert {^state, {:error, :malformed_command}} = applied
    end

    test "fails with an empty key", %{state: state, client: client} do
      {_state, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "", "1"})

      assert result == {:error, :invalid_key}
    end

    test "fails with a key over the limit", %{state: state, client: client} do
      {_state, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "123456789", "1"})

      assert result == {:error, :key_too_large}
    end

    test "fails with a value over the limit", %{state: state, client: client} do
      value = String.duplicate("v", 17)

      {_state, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", value})

      assert result == {:error, :value_too_large}
    end

    test "fails a write that passes the data limit", %{state: state, client: client} do
      {first, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      {second, _result} = TestUtils.run(first, {3, 6}, {client, 2, 2}, {:put, "b", "2"})

      {third, result} = TestUtils.run(second, {4, 7}, {client, 3, 3}, {:put, "c", "3"})

      assert {result, third.data_bytes} == {{:error, :space_exceeded}, second.data_bytes}
    end

    test "fails with a stale command", %{state: state, client: client} do
      {first, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      {second, _result} = TestUtils.run(first, {3, 6}, {client, 3, 3}, {:put, "a", "3"})

      {_state, result} = TestUtils.run(second, {4, 7}, {client, 1, 1}, {:put, "a", "1"})

      assert result == {:error, :stale_command}
    end

    test "fails with too many pending", %{state: state, client: client} do
      {first, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      {second, _result} = TestUtils.run(first, {3, 6}, {client, 2, 1}, {:put, "b", "2"})

      {_state, result} = TestUtils.run(second, {4, 7}, {client, 3, 1}, {:put, "c", "3"})

      assert result == {:error, :too_many_pending}
    end

    test "fails a compare and set with a stale revision", %{state: state, client: client} do
      {first, {:ok, revision}} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      op = {:compare_and_set, "a", revision - 1, {:put, "2"}}

      {second, result} = TestUtils.run(first, {3, 6}, {client, 2, 2}, op)
      fetched = Machine.fetch(second, "a")

      assert {result, fetched} == {{:error, {:revision_mismatch, revision}}, {"1", revision}}
    end

    test "fails a compare and set delete of an absent key", %{state: state, client: client} do
      op = {:compare_and_set, "a", 0, :delete}

      {_state, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, op)

      assert result == {:error, {:revision_mismatch, 0}}
    end

    test "fails a scan with no limit", %{state: state, client: client} do
      {_state, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:range, "a", nil, 0})

      assert result == {:error, :invalid_limit}
    end

    test "fails a limit that is out of range", %{state: state, client: client} do
      op = {:set_limits, %{max_key_bytes: 0}}

      {same, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, op)

      assert {result, same.limits} == {{:error, :invalid_limits}, state.limits}
    end

    test "fails a data limit below the data held", %{state: state, client: client} do
      {first, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      op = {:set_limits, %{max_data_bytes: 10}}

      {_state, result} = TestUtils.run(first, {3, 6}, {client, 2, 2}, op)

      assert result == {:error, :invalid_limits}
    end

    test "returns the first result for a duplicate", %{state: state, client: client} do
      {first, {:ok, revision}} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      {second, _result} = TestUtils.run(first, {3, 6}, {client, 2, 1}, {:put, "a", "2"})

      {third, result} = TestUtils.run(second, {4, 7}, {client, 1, 1}, {:put, "a", "1"})
      fetched = Machine.fetch(third, "a")

      assert {result, fetched} == {{:ok, revision}, {"2", 3}}
    end

    test "returns the index of a put", %{state: state, client: client} do
      {next, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      fetched = Machine.fetch(next, "a")

      assert {result, fetched} == {{:ok, 2}, {"1", 2}}
    end

    test "returns the value and revision on a get", %{state: state, client: client} do
      {first, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})

      {_state, result} = TestUtils.run(first, {3, 6}, {client, 2, 2}, {:get, "a"})

      assert result == {:ok, {"1", 2}}
    end

    test "returns nothing for a get of an absent key", %{state: state, client: client} do
      {_state, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:get, "a"})

      assert result == {:ok, nil}
    end

    test "returns revision 0 for a delete of an absent key", %{state: state, client: client} do
      {next, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:delete, "a"})

      assert {result, next.revision} == {{:ok, 0}, state.revision}
    end

    test "removes a key on delete", %{state: state, client: client} do
      {first, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})

      {next, result} = TestUtils.run(first, {3, 6}, {client, 2, 2}, {:delete, "a"})
      fetched = Machine.fetch(next, "a")

      assert {result, fetched, next.data_bytes} == {{:ok, 3}, nil, 0}
    end

    test "creates a key with a compare and set on revision 0", %{state: state, client: client} do
      op = {:compare_and_set, "a", 0, {:put, "1"}}

      {next, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, op)
      fetched = Machine.fetch(next, "a")

      assert {result, fetched} == {{:ok, 2}, {"1", 2}}
    end

    test "removes a key with a compare and set delete", %{state: state, client: client} do
      {first, {:ok, revision}} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      op = {:compare_and_set, "a", revision, :delete}

      {next, result} = TestUtils.run(first, {3, 6}, {client, 2, 2}, op)
      fetched = Machine.fetch(next, "a")

      assert {result, fetched} == {{:ok, 3}, nil}
    end

    test "reads a page of a range", %{client: client} do
      {store, _client} = TestUtils.populated(5, ~w[a b c d e], @scan)

      {_state, result} = TestUtils.run(store, {7, 20}, {client, 100, 100}, {:range, "b", nil, 2})

      assert result == {:ok, {[{"b", "b", 3}, {"c", "c", 4}], "d"}}
    end

    test "stops a range before its end key", %{client: client} do
      {store, _client} = TestUtils.populated(5, ~w[a b c d e], @scan)

      {_state, result} = TestUtils.run(store, {7, 20}, {client, 100, 100}, {:range, "b", "d", 3})

      assert result == {:ok, {[{"b", "b", 3}, {"c", "c", 4}], nil}}
    end

    test "caps a range at the range limit", %{client: client} do
      {store, _client} = TestUtils.populated(5, ~w[a b c d e], @scan)

      {_state, result} =
        TestUtils.run(store, {7, 20}, {client, 100, 100}, {:range, "a", nil, 100})

      assert {:ok, {[{"a", "a", 2}, {"b", "b", 3}, {"c", "c", 4}], "d"}} = result
    end

    test "reads keys after a key within a prefix", %{client: client} do
      {store, _client} = TestUtils.populated(5, ~w[ab1 ab2 ab3 ac1 b1], @scan)

      {_state, result} =
        TestUtils.run(store, {7, 20}, {client, 100, 100}, {:prefix, "ab", "ab1", 5})

      assert result == {:ok, {[{"ab2", "ab2", 3}, {"ab3", "ab3", 4}], nil}}
    end

    test "changes limits at its index", %{state: state, client: client} do
      op = {:set_limits, %{max_key_bytes: 4, session_ttl_ms: 50}}

      {next, result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, op)

      assert {result, next.limits.max_key_bytes, next.limits.session_ttl_ms} == {{:ok, 2}, 4, 50}
    end

    test "does nothing on a no-op entry", %{state: state} do
      entry = %Entry{term: 1, index: 2, kind: :noop, data: <<>>, time_ms: 5}

      applied = Machine.apply(state, entry)

      assert {^state, {:ok, nil}} = applied
    end
  end

  describe "sessions" do
    test "opens a session at the index of its register", %{state: state, client: client} do
      count = :gb_trees.size(state.sessions)

      assert {client, count} == {1, 1}
    end

    test "evicts the least recently seen session when full", %{state: state, client: client} do
      {second, other} = TestUtils.register(state, 2, 10)
      {third, _client} = TestUtils.register(second, 3, 20)
      {touched, _result} = TestUtils.run(third, {4, 30}, {client, 1, 1}, {:get, "a"})

      {fourth, _client} = TestUtils.register(touched, 5, 40)

      result = TestUtils.session_ids(fourth)

      assert result == [client, 3, 5] and other == 2
    end

    test "evicts the lower id on a tie", %{state: state} do
      {second, _client} = TestUtils.register(state, 2, 0)
      {third, _client} = TestUtils.register(second, 3, 0)

      {fourth, _client} = TestUtils.register(third, 4, 0)

      result = TestUtils.session_ids(fourth)

      assert result == [2, 3, 4]
    end

    test "expires idle sessions at the sweep index", %{state: state, client: client} do
      {second, other} = TestUtils.register(state, 2, 4_500)

      {swept, _result} = TestUtils.run(second, {10, 5_000}, {other, 1, 1}, {:get, "a"})

      result = TestUtils.session_ids(swept)

      assert result == [other] and client == 1
    end

    test "keeps idle sessions before the sweep index", %{state: state, client: client} do
      {second, other} = TestUtils.register(state, 2, 4_500)

      {kept, _result} = TestUtils.run(second, {9, 5_000}, {other, 1, 1}, {:get, "a"})

      result = TestUtils.session_ids(kept)

      assert result == [client, other]
    end

    test "fails a command of an expired session", %{state: state, client: client} do
      {second, other} = TestUtils.register(state, 2, 4_500)
      {swept, _result} = TestUtils.run(second, {10, 5_000}, {other, 1, 1}, {:get, "a"})

      {_state, result} = TestUtils.run(swept, {11, 5_001}, {client, 1, 1}, {:put, "a", "1"})

      assert result == {:error, :session_expired}
    end

    test "sweeps at the same entry on every member", %{state: state} do
      {second, other} = TestUtils.register(state, 2, 4_500)

      {first_member, _result} = TestUtils.run(second, {10, 5_000}, {other, 1, 1}, {:get, "a"})
      {second_member, _result} = TestUtils.run(second, {10, 5_000}, {other, 1, 1}, {:get, "a"})

      first_snapshot = Machine.snapshot(first_member)
      second_snapshot = Machine.snapshot(second_member)

      assert first_snapshot == second_snapshot
    end
  end

  describe "restore/1" do
    test "fails on bytes that are not a snapshot" do
      restored = Machine.restore("junk")

      assert {:error, :corrupt_snapshot} = restored
    end

    test "keeps records and limits", %{state: state, client: client} do
      {first, _result} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      op = {:set_limits, %{max_key_bytes: 4}}
      {second, _result} = TestUtils.run(first, {3, 6}, {client, 2, 2}, op)
      snapshot = Machine.snapshot(second)

      {:ok, restored} = Machine.restore(snapshot)
      fetched = Machine.fetch(restored, "a")

      assert {fetched, restored.limits.max_key_bytes} == {{"1", 2}, 4}
    end

    test "keeps the stored results of sessions", %{state: state, client: client} do
      {first, {:ok, revision}} = TestUtils.run(state, {2, 5}, {client, 1, 1}, {:put, "a", "1"})
      {:ok, restored} = first |> Machine.snapshot() |> Machine.restore()

      {_state, result} = TestUtils.run(restored, {3, 6}, {client, 1, 1}, {:put, "a", "9"})

      assert result == {:ok, revision}
    end
  end
end
