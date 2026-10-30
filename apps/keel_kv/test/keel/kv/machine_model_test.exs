defmodule Keel.KV.MachineModelTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Keel.KV.Command
  alias Keel.KV.Machine
  alias Keel.KV.TestUtils

  describe "decode/1" do
    property "never raises on random bytes" do
      check all(bytes <- StreamData.binary(max_length: 80), max_runs: 300) do
        class = TestUtils.decode_class(bytes)

        assert class in [:ok, :malformed]
      end
    end

    property "fails on every cut of a command" do
      check all(command <- TestUtils.random_command(), max_runs: 200) do
        bytes = Command.encode(command)

        cuts = TestUtils.decoding_prefixes(bytes)

        assert cuts == []
      end
    end

    property "returns the command that was encoded" do
      check all(command <- TestUtils.random_command(), max_runs: 300) do
        bytes = Command.encode(command)

        decoded = Command.decode(bytes)

        assert decoded == {:ok, command}
      end
    end
  end

  describe "apply/2" do
    property "gives an error and never raises on random bytes" do
      check all(bytes <- StreamData.binary(max_length: 80), max_runs: 300) do
        {state, _client} = TestUtils.machine_after([])

        outcome = TestUtils.apply_bytes(state, bytes)

        assert outcome in [:ok, :error]
      end
    end

    property "agrees with a plain map" do
      ops = StreamData.list_of(TestUtils.random_write(), max_length: 60)

      check all(ops <- ops, max_runs: 200) do
        mismatches = TestUtils.model_mismatches(ops)

        assert mismatches == []
      end
    end

    property "gives equal snapshots to two machines fed the same ops" do
      ops = StreamData.list_of(TestUtils.random_write(), max_length: 60)

      check all(ops <- ops, max_runs: 100) do
        {first, _client} = TestUtils.machine_after(ops)
        {second, _client} = TestUtils.machine_after(ops)

        first_snapshot = Machine.snapshot(first)
        second_snapshot = Machine.snapshot(second)

        assert first_snapshot == second_snapshot
      end
    end
  end

  describe "restore/1" do
    property "returns a state with the same snapshot" do
      ops = StreamData.list_of(TestUtils.random_write(), max_length: 60)

      check all(ops <- ops, max_runs: 100) do
        {state, _client} = TestUtils.machine_after(ops)
        snapshot = Machine.snapshot(state)
        {:ok, restored} = Machine.restore(snapshot)
        restored_snapshot = Machine.snapshot(restored)

        assert restored_snapshot == snapshot
      end
    end

    property "returns a state that goes on like the original" do
      ops = StreamData.list_of(TestUtils.random_write(), max_length: 30)

      check all(before <- ops, later <- ops, max_runs: 100) do
        {state, client} = TestUtils.machine_after(before)
        {:ok, restored} = state |> Machine.snapshot() |> Machine.restore()
        first = length(before) + 2

        {original, results} = TestUtils.continue(state, client, first, later)
        {continued, same} = TestUtils.continue(restored, client, first, later)
        continued_snapshot = Machine.snapshot(continued)
        original_snapshot = Machine.snapshot(original)

        assert {continued_snapshot, same} == {original_snapshot, results}
      end
    end
  end
end
