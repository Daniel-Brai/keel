defmodule KeelSim.ReplicationSafetyTest do
  use ExUnit.Case, async: true

  alias KeelSim.TestUtils

  describe "run/1" do
    test "catches acks sent before entries are durable" do
      opts = %{size: 3, steps: 5_000, bug: :ack_before_durable}

      found = TestUtils.first_violation(1..300, opts)

      assert found != nil
    end

    test "keeps every applied history equal under faults" do
      opts = %{size: 5, steps: 5_000}

      seeds = TestUtils.violating_seeds(1..30, opts)

      assert seeds == []
    end

    test "keeps every applied history equal in three member clusters" do
      opts = %{size: 3, steps: 5_000}

      seeds = TestUtils.violating_seeds(201..230, opts)

      assert seeds == []
    end

    test "commits entries without faults" do
      result = KeelSim.run(%{seed: 11, size: 3, steps: 3_000, faults?: false})

      assert result.committed >= 20
    end
  end
end
