defmodule KeelSim.LivenessTest do
  use ExUnit.Case, async: true

  alias KeelSim.Liveness
  alias KeelSim.Scenario
  alias KeelSim.TestUtils

  describe "status/3" do
    test "fails with no leader before any election" do
      cluster = Scenario.play([], %{size: 3})

      status = Liveness.status(cluster.members, cluster.ids, false)

      assert status == {:pending, :no_leader}
    end

    test "fails with no leader until a majority follows" do
      cluster = Scenario.play([{:elect, "m1", ["m2"]}], %{size: 3})

      status = Liveness.status(cluster.members, cluster.ids, false)

      assert status == {:pending, :no_leader}
    end

    test "fails with no commit before a command committed" do
      script = [{:elect, "m1", ["m2"]}, {:exchange, "m1", "m2"}]
      cluster = Scenario.play(script, %{size: 3})

      status = Liveness.status(cluster.members, cluster.ids, false)

      assert status == {:pending, :no_commit}
    end

    test "fails when a member lags behind" do
      script = [{:elect, "m1", ["m2"]}, {:sync, "m1"}, {:exchange, "m1", "m2"}]
      cluster = Scenario.play(script, %{size: 3})

      status = Liveness.status(cluster.members, cluster.ids, true)

      assert status == {:pending, :not_converged}
    end
  end

  describe "leader/2" do
    test "returns the leader that a majority follows" do
      script = [{:elect, "m1", ["m2"]}, {:exchange, "m1", "m2"}]
      cluster = Scenario.play(script, %{size: 3})

      found = Liveness.leader(cluster.members, cluster.ids)

      assert {:ok, %{id: "m1"}} = found
    end
  end

  describe "a run" do
    test "reports a liveness miss apart from safety violations" do
      result = KeelSim.run(%{seed: 1, size: 3, steps: 0, heal_steps: 0})

      assert {result.violations, result.liveness} == {[], [liveness: :no_leader]}
    end

    test "recovers after faults stop" do
      opts = %{steps: 3_000}

      seeds = TestUtils.liveness_failures(1..30, opts)

      assert seeds == []
    end

    test "recovers in five member clusters" do
      opts = %{size: 5, steps: 3_000}

      seeds = TestUtils.liveness_failures(31..50, opts)

      assert seeds == []
    end
  end
end
