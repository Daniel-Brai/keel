defmodule KeelSim.ShrinkTest do
  use ExUnit.Case, async: true

  alias KeelSim.Shrink
  alias KeelSim.TestUtils

  @opts %{size: 3, steps: 4_000, bug: :ack_before_durable}

  describe "class/1" do
    test "returns nothing for a passing run" do
      result = KeelSim.run(%{seed: 1, size: 3, steps: 300})

      class = Shrink.class(result)

      assert class == nil
    end

    test "names the first violation" do
      result = %{violations: [{:acknowledged_loss, 3}, {:vote_changed, "m1", 2}], liveness: []}

      class = Shrink.class(result)

      assert class == :acknowledged_loss
    end

    test "names a liveness miss" do
      result = %{violations: [], liveness: [{:liveness, :no_leader}]}

      class = Shrink.class(result)

      assert class == {:liveness, :no_leader}
    end
  end

  describe "shrink/3" do
    test "returns a schedule that does not fail unchanged" do
      schedule = [{:tick, "m1"}, {:tick, "m2"}]

      shrunk = Shrink.shrink(%{seed: 1, size: 3}, schedule, :acknowledged_loss)

      assert shrunk == schedule
    end

    test "cuts a failing schedule to under a third" do
      failing = TestUtils.shortest_failing_run(1..40, @opts)
      class = Shrink.class(failing)

      shrunk = Shrink.shrink(failing.config, failing.schedule, class)

      original = length(failing.schedule)
      smaller = length(shrunk) * 3

      assert smaller < original
    end

    test "keeps the failure class" do
      failing = TestUtils.shortest_failing_run(1..40, @opts)
      class = Shrink.class(failing)
      shrunk = Shrink.shrink(failing.config, failing.schedule, class)

      replayed = KeelSim.replay(failing.config, shrunk, false)

      replayed_class = Shrink.class(replayed)

      assert replayed_class == class
    end

    test "gives the same schedule every time" do
      failing = TestUtils.shortest_failing_run(1..40, @opts)
      class = Shrink.class(failing)

      first = Shrink.shrink(failing.config, failing.schedule, class)
      second = Shrink.shrink(failing.config, failing.schedule, class)

      assert first == second
    end

    test "cuts a liveness miss to under a third" do
      opts = %{size: 3, steps: 300, heal_steps: 0}
      seeded = Map.put(opts, :seed, 2)
      result = KeelSim.run(seeded)
      class = Shrink.class(result)

      shrunk = Shrink.shrink(result.config, result.schedule, class)

      original = length(result.schedule)
      smaller = length(shrunk) * 3

      assert smaller < original
    end
  end
end
