defmodule Keel.Raft.NodeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Keel.Raft.Config
  alias Keel.Raft.Node
  alias Keel.TestUtils

  @opts %{
    id: "m1",
    voters: ~w[m3 m1 m2],
    seed: {1, 2, 3},
    election_ticks_min: 10,
    election_ticks_max: 20
  }

  describe "new/1" do
    test "draws the same timeout from the same seed" do
      first = Node.new(@opts)
      second = Node.new(@opts)

      assert first.election_timeout == second.election_timeout
    end

    property "draws a timeout inside the range" do
      check all(seed <- {positive_integer(), positive_integer(), positive_integer()}) do
        node = @opts |> Map.put(:seed, seed) |> Node.new()

        assert node.election_timeout in 10..20
      end
    end

    test "takes the config of the newest config entry in its log" do
      older = TestUtils.random_config()
      newer = TestUtils.random_config(learners: ["m4"])
      entries = [TestUtils.config_entry(1, 0, older), TestUtils.config_entry(2, 1, newer)]

      node = @opts |> Map.put(:entries, entries) |> Node.new()

      assert node.config == newer
    end

    test "falls back to the voters it was given" do
      node = Node.new(@opts)

      assert node.config == TestUtils.random_config(voters: ~w[m1 m2 m3])
    end

    test "sorts the voters" do
      node = Node.new(@opts)

      assert Config.voters(node.config) == ~w[m1 m2 m3]
    end
  end

  describe "step/2" do
    test "counts a tick with no effects" do
      node = Node.new(@opts)

      stepped = Node.step(node, :tick)

      assert {%Node{election_elapsed: 1}, []} = stepped
    end
  end
end
