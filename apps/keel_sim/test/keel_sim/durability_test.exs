defmodule KeelSim.DurabilityTest do
  use ExUnit.Case, async: true

  alias KeelSim.TestUtils

  describe "run/1" do
    test "catches acks sent before entries are durable" do
      opts = %{size: 3, steps: 5_000, bug: :ack_before_durable}

      seeds = TestUtils.caught_seeds(1..40, opts)

      assert seeds != []
    end

    test "reports an acknowledged entry that a majority lost" do
      opts = %{
        size: 3,
        steps: 5_000,
        bug: :ack_before_durable,
        crash_profile: :strict,
        check_acks: false
      }

      seeds = TestUtils.loss_seeds(1..40, opts)

      assert seeds != []
    end

    test "keeps every acknowledged entry on a strict disk" do
      opts = %{size: 3, steps: 4_000, crash_profile: :strict}

      seeds = TestUtils.violating_seeds(1..20, opts)

      assert seeds == []
    end

    test "keeps every acknowledged entry on a prefix disk" do
      opts = %{size: 3, steps: 4_000, crash_profile: :prefix}

      seeds = TestUtils.violating_seeds(1..20, opts)

      assert seeds == []
    end

    test "keeps every acknowledged entry on a subset disk" do
      opts = %{size: 3, steps: 4_000, crash_profile: :subset}

      seeds = TestUtils.violating_seeds(1..20, opts)

      assert seeds == []
    end

    test "keeps every acknowledged entry on a torn disk" do
      opts = %{size: 3, steps: 4_000, crash_profile: :torn}

      seeds = TestUtils.violating_seeds(1..20, opts)

      assert seeds == []
    end

    test "keeps every acknowledged entry in five member clusters" do
      opts = %{size: 5, steps: 4_000}

      seeds = TestUtils.violating_seeds(401..420, opts)

      assert seeds == []
    end

    test "gives the same trace for the same seed on a torn disk" do
      opts = %{seed: 77, size: 3, steps: 3_000, crash_profile: :torn}

      first = KeelSim.run(opts)
      second = KeelSim.run(opts)

      assert first == second
    end

    test "rotates segments without losing entries" do
      opts = %{size: 3, steps: 4_000, segment_bytes: 256}

      seeds = TestUtils.violating_seeds(1..20, opts)

      assert seeds == []
    end
  end
end
