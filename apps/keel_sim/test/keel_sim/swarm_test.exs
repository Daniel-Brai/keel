defmodule KeelSim.SwarmTest do
  use ExUnit.Case, async: true

  alias KeelSim.Swarm

  describe "resolve/1" do
    test "keeps an explicit size" do
      config = Swarm.resolve(%{seed: 4, size: 7})

      assert config.size == 7
    end

    test "keeps explicit faults" do
      config = Swarm.resolve(%{seed: 4, faults: [:drop]})

      assert config.faults == [:drop]
    end

    test "turns every fault off without faults" do
      config = Swarm.resolve(%{seed: 4, faults?: false})

      assert {config.faults, config.fault_rate} == {[], 0}
    end

    test "overrides one weight and keeps the others" do
      drawn = Swarm.draw(4)
      expected = %{drawn.weights | tick: 1}

      config = Swarm.resolve(%{seed: 4, weights: %{tick: 1}})

      assert config.weights == expected
    end

    test "gives the same configuration for the same seed" do
      first = Swarm.resolve(%{seed: 9})
      second = Swarm.resolve(%{seed: 9})

      assert first == second
    end

    test "resolves a resolved configuration to itself" do
      config = Swarm.resolve(%{seed: 9})

      again = Swarm.resolve(config)

      assert again == config
    end
  end

  describe "draw/1" do
    test "differs across seeds" do
      draws = Enum.map(1..30, &Swarm.draw/1)

      distinct = draws |> Enum.uniq() |> length()

      assert distinct == 30
    end

    test "draws only sizes of one three or five" do
      draws = Enum.map(1..200, &Swarm.draw/1)

      sizes = draws |> Enum.map(& &1.size) |> Enum.uniq() |> Enum.sort()

      assert sizes == [1, 3, 5]
    end

    test "draws every fault kind in some seed" do
      draws = Enum.map(1..200, &Swarm.draw/1)

      faults = draws |> Enum.flat_map(& &1.faults) |> Enum.uniq() |> Enum.sort()

      assert faults == ~w[crash drop duplicate membership partition pause transfer]a
    end

    test "draws every crash profile" do
      draws = Enum.map(1..200, &Swarm.draw/1)

      profiles = draws |> Enum.map(& &1.crash_profile) |> Enum.uniq() |> Enum.sort()

      assert profiles == ~w[prefix strict subset torn]a
    end

    test "draws both workloads" do
      draws = Enum.map(1..200, &Swarm.draw/1)

      workloads = draws |> Enum.map(& &1.workload) |> Enum.uniq() |> Enum.sort()

      assert workloads == [:chain, :kv]
    end

    test "draws the key value workload most often" do
      draws = Enum.map(1..400, &Swarm.draw/1)

      chains = Enum.count(draws, &(&1.workload == :chain))

      assert chains in 60..140
    end

    test "draws every client count" do
      draws = Enum.map(1..200, &Swarm.draw/1)

      counts = draws |> Enum.map(& &1.clients) |> Enum.uniq() |> Enum.sort()

      assert counts == [2, 3, 4]
    end

    test "draws election ranges that stay valid" do
      draws = Enum.map(1..200, &Swarm.draw/1)

      valid? = Enum.all?(draws, &(&1.election_ticks_min <= &1.election_ticks_max))

      assert valid?
    end
  end
end
