defmodule Keel.Chain.MachineTest do
  use ExUnit.Case, async: true

  alias Keel.Chain.Machine
  alias Keel.Raft.Entry

  describe "restore/1" do
    test "fails on bytes of the wrong size" do
      restored = Machine.restore(<<1, 2, 3>>)

      assert {:error, :corrupt_snapshot} = restored
    end

    test "returns the state that was saved" do
      entry = %Entry{term: 1, index: 1, kind: :command, data: "block", time_ms: 0}
      empty = Machine.init(%{})
      {state, _result} = Machine.apply(empty, entry)

      snapshot = Machine.snapshot(state)

      restored = Machine.restore(snapshot)

      assert restored == {:ok, state}
    end
  end

  describe "apply/2" do
    test "gives another hash at another index" do
      first = %Entry{term: 1, index: 1, kind: :command, data: "block", time_ms: 0}
      second = %Entry{first | index: 2}
      empty = Machine.init(%{})

      {_state, one} = Machine.apply(empty, first)
      {_state, two} = Machine.apply(empty, second)

      assert one != two
    end

    test "returns the count and the hash" do
      entry = %Entry{term: 1, index: 1, kind: :command, data: "block", time_ms: 0}

      empty = Machine.init(%{})

      {state, result} = Machine.apply(empty, entry)

      assert result == {:ok, {1, state.hash}}
    end
  end
end
