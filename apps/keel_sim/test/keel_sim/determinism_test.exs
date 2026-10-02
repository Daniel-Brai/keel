defmodule KeelSim.DeterminismTest do
  use ExUnit.Case, async: true

  alias KeelSim.TestUtils

  describe "run/1" do
    test "differs for different seeds" do
      older = KeelSim.run(%{seed: 1, size: 3, steps: 2_000})
      newer = KeelSim.run(%{seed: 2, size: 3, steps: 2_000})

      assert older.trace_hash != newer.trace_hash
    end

    test "gives the same trace for the same seed" do
      opts = %{seed: 8_812_734, size: 5, steps: 5_000}

      first = KeelSim.run(opts)
      second = KeelSim.run(opts)

      assert first == second
    end

    test "gives the same trace twice for twenty swarm seeds" do
      opts = %{steps: 1_500}

      first = TestUtils.swept(1..20, opts)
      second = TestUtils.swept(1..20, opts)

      first_hashes = Enum.map(first, & &1.trace_hash)
      second_hashes = Enum.map(second, & &1.trace_hash)

      assert first_hashes == second_hashes
    end

    test "draws different clusters across swarm seeds" do
      results = TestUtils.swept(1..20, %{steps: 100})

      sizes = results |> Enum.map(& &1.config.size) |> Enum.uniq()
      count = length(sizes)

      assert count == 3
    end

    test "replays a schedule to the same trace" do
      result = KeelSim.run(%{seed: 12, steps: 2_000})

      replayed = KeelSim.replay(result.config, result.schedule)

      assert replayed.trace_hash == result.trace_hash
    end

    test "replays a schedule that skips actions that are not enabled" do
      result = KeelSim.run(%{seed: 12, size: 3, steps: 500})
      noisy = [{:restart, "m1"}, {:deliver, {"m1", "m2"}} | result.schedule]

      replayed = KeelSim.replay(result.config, noisy)

      assert replayed.violations == []
    end

    test "runs every step" do
      result = KeelSim.run(%{seed: 3, size: 3, steps: 300})

      assert %{steps: 300} = result
    end
  end
end
