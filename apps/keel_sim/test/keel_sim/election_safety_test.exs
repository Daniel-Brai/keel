defmodule KeelSim.ElectionSafetyTest do
  use ExUnit.Case, async: true

  alias KeelSim.TestUtils

  describe "run/1" do
    test "catches a double vote bug" do
      opts = %{size: 3, steps: 3_000, bug: :double_vote}

      found = TestUtils.first_violation(1..200, opts)

      assert found != nil
    end

    test "keeps one leader per term under faults" do
      opts = %{size: 5, steps: 4_000}

      seeds = TestUtils.violating_seeds(1..30, opts)

      assert seeds == []
    end

    test "keeps one leader per term in three member clusters" do
      opts = %{size: 3, steps: 4_000}

      seeds = TestUtils.violating_seeds(201..230, opts)

      assert seeds == []
    end

    test "elects a leader without faults" do
      result = KeelSim.run(%{seed: 7, size: 3, steps: 2_000, faults?: false})

      assert result.elections >= 1
    end
  end
end
