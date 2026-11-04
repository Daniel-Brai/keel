defmodule KeelSim.LinearizableTest do
  use ExUnit.Case, async: true

  alias KeelSim.Linearizable
  alias KeelSim.TestUtils

  describe "check/2" do
    test "fails a read of an older value after a newer write" do
      ops = [
        TestUtils.put(1, {0, 10}, "one", 5),
        TestUtils.put(1, {20, 30}, "two", 7),
        TestUtils.get(2, {40, 50}, {"one", 5})
      ]

      verdict = Linearizable.check(ops)

      assert {:error, {:not_linearizable, "k", _window}} = verdict
    end

    test "fails a read of a value nobody wrote" do
      ops = [TestUtils.get(1, {0, 10}, {"ghost", 3})]

      verdict = Linearizable.check(ops)

      assert {:error, {:not_linearizable, "k", _window}} = verdict
    end

    test "fails a read of nothing after an acknowledged write" do
      ops = [TestUtils.put(1, {0, 10}, "one", 5), TestUtils.get(2, {20, 30}, nil)]

      verdict = Linearizable.check(ops)

      assert {:error, {:not_linearizable, "k", _window}} = verdict
    end

    test "fails a write that took effect twice" do
      ops = [
        TestUtils.put(1, {0, 10}, "one", 4),
        TestUtils.get(2, {20, 30}, {"one", 4}),
        TestUtils.get(2, {40, 50}, {"one", 9})
      ]

      verdict = Linearizable.check(ops)

      assert {:error, {:not_linearizable, "k", _window}} = verdict
    end

    test "fails a compare and set that won twice" do
      ops = [
        TestUtils.compare_and_set(1, {0, 30}, 0, "one", {:ok, 5}),
        TestUtils.compare_and_set(2, {0, 30}, 0, "two", {:ok, 6})
      ]

      verdict = Linearizable.check(ops)

      assert {:error, {:not_linearizable, "k", _window}} = verdict
    end

    test "fails a revision that goes backwards" do
      ops = [TestUtils.put(1, {0, 10}, "one", 9), TestUtils.put(1, {20, 30}, "two", 4)]

      verdict = Linearizable.check(ops)

      assert {:error, {:not_linearizable, "k", _window}} = verdict
    end

    test "fails a mismatch that names a revision nobody wrote" do
      ops = [
        TestUtils.put(1, {0, 10}, "one", 5),
        TestUtils.compare_and_set(2, {20, 30}, 2, "two", {:error, {:revision_mismatch, 8}})
      ]

      verdict = Linearizable.check(ops)

      assert {:error, {:not_linearizable, "k", _window}} = verdict
    end

    test "names the key that failed" do
      good = TestUtils.put(1, {0, 10}, "one", 5, "a")
      bad = TestUtils.get(1, {20, 30}, {"ghost", 3}, "b")

      verdict = Linearizable.check([good, bad])

      assert {:error, {:not_linearizable, "b", _window}} = verdict
    end

    test "returns unknown when the search passes its limit" do
      ops = TestUtils.random_history(60, 4, 1)

      verdict = Linearizable.check(ops, max_states: 10)

      assert verdict == {:unknown, "k"}
    end

    test "returns ok for ops in sequence" do
      ops = [
        TestUtils.put(1, {0, 10}, "one", 5),
        TestUtils.get(2, {20, 30}, {"one", 5}),
        TestUtils.compare_and_set(1, {40, 50}, 5, "two", {:ok, 9}),
        TestUtils.get(2, {60, 70}, {"two", 9})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for a read that overlaps a write and sees the old state" do
      ops = [TestUtils.put(1, {0, 30}, "one", 5), TestUtils.get(2, {10, 20}, nil)]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for a read that overlaps a write and sees the new state" do
      ops = [TestUtils.put(1, {0, 30}, "one", 5), TestUtils.get(2, {10, 20}, {"one", 5})]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for an unknown write that took effect late" do
      late = TestUtils.put(2, {20, 30}, "two", 0)

      ops = [
        TestUtils.put(1, {0, 10}, "one", 3),
        TestUtils.unknown(late),
        TestUtils.get(1, {100, 110}, {"two", 8})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for an unknown write that never took effect" do
      late = TestUtils.put(2, {20, 30}, "two", 0)

      ops = [
        TestUtils.put(1, {0, 10}, "one", 3),
        TestUtils.unknown(late),
        TestUtils.get(1, {100, 110}, {"one", 3})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok when an unknown write lands after later reads" do
      late = TestUtils.put(2, {20, 30}, "two", 0)

      ops = [
        TestUtils.put(1, {0, 10}, "one", 3),
        TestUtils.unknown(late),
        TestUtils.get(1, {100, 110}, {"one", 3}),
        TestUtils.get(1, {200, 210}, {"two", 12})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for an unknown compare and set that won" do
      late = TestUtils.compare_and_set(2, {20, 30}, 3, "two", nil)

      ops = [
        TestUtils.put(1, {0, 10}, "one", 3),
        TestUtils.unknown(late),
        TestUtils.get(1, {100, 110}, {"two", 6})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for a mismatch that names the current revision" do
      ops = [
        TestUtils.put(1, {0, 10}, "one", 5),
        TestUtils.compare_and_set(2, {20, 30}, 2, "two", {:error, {:revision_mismatch, 5}})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for a create after a delete" do
      ops = [
        TestUtils.put(1, {0, 10}, "one", 5),
        TestUtils.delete(1, {20, 30}, 6),
        TestUtils.get(2, {40, 50}, nil),
        TestUtils.compare_and_set(1, {60, 70}, 0, "two", {:ok, 9})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for a delete of an absent key" do
      ops = [TestUtils.delete(1, {0, 10}, 0), TestUtils.get(1, {20, 30}, nil)]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "ignores an operation that failed" do
      refused = TestUtils.put(2, {20, 30}, "two", 0)

      ops = [
        TestUtils.put(1, {0, 10}, "one", 3),
        TestUtils.failed(refused),
        TestUtils.get(1, {40, 50}, {"one", 3})
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "checks each key alone" do
      ops = [
        TestUtils.put(1, {0, 10}, "one", 5, "a"),
        TestUtils.put(1, {20, 30}, "two", 3, "b"),
        TestUtils.get(2, {40, 50}, {"one", 5}, "a"),
        TestUtils.get(2, {60, 70}, {"two", 3}, "b")
      ]

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end

    test "returns ok for 2000 ops from 8 processes on one key" do
      ops = TestUtils.random_history(2_000, 8, 7)

      verdict = Linearizable.check(ops)

      assert verdict == :ok
    end
  end
end
