defmodule KeelSim.CoverageTest do
  use ExUnit.Case, async: true

  alias KeelSim.Coverage
  alias KeelSim.TestUtils

  describe "missing/1" do
    test "names every counter at zero" do
      empty = Coverage.new()

      missing = Coverage.missing(empty)

      names = Coverage.names()

      assert missing == names
    end

    test "leaves out a counter that was hit" do
      empty = Coverage.new()
      hit = Coverage.hit(empty, :noop_commit)

      missing = Coverage.missing(hit)

      assert :noop_commit not in missing
    end
  end

  describe "merge/2" do
    test "adds the counts" do
      empty = Coverage.new()
      once = Coverage.hit(empty, :noop_commit)

      merged = Coverage.merge(once, once)

      assert merged.noop_commit == 2
    end
  end

  describe "a run" do
    test "returns every counter" do
      result = KeelSim.run(%{seed: 5, steps: 200})

      counted = result.coverage |> Map.keys() |> Enum.sort()
      names = Enum.sort(Coverage.names())

      assert counted == names
    end

    test "counts leaders without faults" do
      result = KeelSim.run(%{seed: 5, size: 3, steps: 500, faults?: false})

      assert result.coverage.leader_elected >= 1
    end
  end

  describe "a sweep" do
    test "reaches every named path" do
      coverage = TestUtils.coverage_after(1..160, %{steps: 3_000})

      missing = Coverage.missing(coverage)

      assert missing == []
    end
  end
end
