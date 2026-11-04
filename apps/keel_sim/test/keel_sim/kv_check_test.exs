defmodule KeelSim.KvCheckTest do
  use ExUnit.Case, async: true

  alias KeelSim.KvCheck
  alias KeelSim.TestUtils

  describe "check/2" do
    test "reports a read with a bad checksum" do
      good = KvCheck.value(1, 1)
      <<_crc::32, payload::binary>> = good
      forged = <<0::32, payload::binary>>

      ops = [
        TestUtils.put(1, {0, 10}, forged, 5, "s/0"),
        TestUtils.get(2, {20, 30}, {forged, 5}, "s/0")
      ]

      violations = KvCheck.check(ops)

      assert {:garbage_read, "s/0"} in violations
    end

    test "reports a read of a value that no write stored" do
      value = KvCheck.value(1, 1)
      stranger = KvCheck.value(2, 9)

      ops = [
        TestUtils.put(1, {0, 10}, value, 5, "s/0"),
        TestUtils.get(2, {20, 30}, {stranger, 5}, "s/0")
      ]

      violations = KvCheck.check(ops)

      assert {:garbage_read, "s/0"} in violations
    end

    test "reports a lost write on the key of one client" do
      value = KvCheck.value(1, 1)

      ops = [
        TestUtils.put(1, {0, 10}, value, 5, "p/1/0"),
        TestUtils.get(1, {20, 30}, nil, "p/1/0")
      ]

      violations = KvCheck.check(ops)

      assert {:lost_write, "p/1/0"} in violations
    end

    test "reports a stale read after an acknowledged write" do
      first = KvCheck.value(1, 1)
      second = KvCheck.value(2, 1)

      ops = [
        TestUtils.put(1, {0, 10}, first, 5, "s/0"),
        TestUtils.put(2, {20, 30}, second, 9, "s/0"),
        TestUtils.get(3, {40, 50}, {first, 5}, "s/0")
      ]

      violations = KvCheck.check(ops)

      assert {:stale_read, "s/0"} in violations
    end

    test "reports two winners of one compare and set round" do
      first = KvCheck.value(1, 1)
      second = KvCheck.value(2, 1)

      ops = [
        TestUtils.compare_and_set(1, {0, 30}, 0, first, {:ok, 5}, "r/1"),
        TestUtils.compare_and_set(2, {0, 30}, 0, second, {:ok, 6}, "r/1")
      ]

      violations = KvCheck.check(ops)

      assert {:double_compare_and_set_win, "r/1"} in violations
    end

    test "reports a history that is not linearizable" do
      value = KvCheck.value(1, 1)

      ops = [
        TestUtils.put(1, {0, 10}, value, 4, "s/0"),
        TestUtils.get(2, {20, 30}, {value, 4}, "s/0"),
        TestUtils.get(2, {40, 50}, {value, 9}, "s/0")
      ]

      violations = KvCheck.check(ops)

      assert violations == [{:not_linearizable, "s/0"}]
    end

    test "reports a history that is too large to search" do
      ops = TestUtils.random_history(60, 4, 1)

      violations = KvCheck.check(ops, max_states: 10)

      assert {:linearizability_unknown, "k"} in violations
    end

    test "accepts an empty read while a delete overlaps it" do
      value = KvCheck.value(1, 1)

      ops = [
        TestUtils.put(1, {0, 10}, value, 4, "s/0"),
        TestUtils.delete(2, {20, 60}, 7, "s/0"),
        TestUtils.get(3, {30, 40}, nil, "s/0")
      ]

      violations = KvCheck.check(ops)

      assert violations == []
    end

    test "accepts an unknown write that a later read shows" do
      first = KvCheck.value(1, 1)
      second = KvCheck.value(1, 2)
      late = TestUtils.put(1, {20, 30}, second, 0, "p/1/0")

      ops = [
        TestUtils.put(1, {0, 10}, first, 4, "p/1/0"),
        TestUtils.unknown(late),
        TestUtils.get(1_001, {100, 110}, {second, 8}, "p/1/0")
      ]

      violations = KvCheck.check(ops)

      assert violations == []
    end
  end

  describe "valid_value?/1" do
    test "fails a value with the wrong checksum" do
      valid? = KvCheck.valid_value?(<<0::32, "payload">>)

      refute valid?
    end

    test "passes a value the workload made" do
      value = KvCheck.value(3, 12)

      valid? = KvCheck.valid_value?(value)

      assert valid?
    end
  end
end
