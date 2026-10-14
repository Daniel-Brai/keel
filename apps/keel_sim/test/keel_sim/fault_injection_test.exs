defmodule KeelSim.FaultInjectionTest do
  use ExUnit.Case, async: true

  alias KeelSim.TestUtils

  describe "run/1" do
    test "never delays a completion when off" do
      coverage = TestUtils.coverage_after(1..10, %{size: 3, steps: 1_500, fault_rate: 0})

      assert coverage.persist_delayed == 0
    end

    test "never fails a sync when off" do
      coverage = TestUtils.coverage_after(1..10, %{size: 3, steps: 1_500, fault_rate: 0})

      assert coverage.sync_failure_injected == 0
    end

    test "delays completions when on" do
      coverage = TestUtils.coverage_after(1..10, %{size: 3, steps: 1_500, fault_rate: 40})

      assert coverage.persist_delayed > 0
    end

    test "fails syncs when on" do
      coverage = TestUtils.coverage_after(1..10, %{size: 3, steps: 1_500, fault_rate: 100})

      assert coverage.sync_failure_injected > 0
    end

    test "crashes the member of a failed sync" do
      coverage = TestUtils.coverage_after(1..10, %{size: 3, steps: 1_500, fault_rate: 100})

      assert coverage.sync_failure_crash >= coverage.sync_failure_injected
    end

    test "keeps every safety check under a high fault rate" do
      opts = %{size: 3, steps: 2_000, fault_rate: 100}

      seeds = TestUtils.failing_seeds(1..20, opts)

      assert seeds == []
    end
  end
end
