defmodule KeelSim.Regression.OldTermEntryNotCommittedByCountTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # Five members. The first leader puts an entry on a second member only. A member that
  # never saw it wins a later term. The first member returns and puts the old entry on a
  # majority. Then a member with a newer last term wins and overwrites the entry.
  @script [
    {:elect, "m1", ~w[m2 m3]},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:exchange, "m1", "m4"},
    {:exchange, "m1", "m5"},
    {:propose, "m1", "x"},
    {:sync, "m1"},
    {:exchange, "m1", "m2"},
    {:drop_all, "m1", "m3"},
    {:drop_all, "m1", "m4"},
    {:drop_all, "m1", "m5"},
    {:crash, "m1"},
    {:elect, "m5", ~w[m3 m4]},
    {:sync, "m5"},
    {:crash, "m5"},
    {:restart, "m1"},
    {:campaign, "m1"},
    {:drop_all, "m1", "m2"},
    {:drop_all, "m1", "m3"},
    {:drop_all, "m1", "m4"},
    {:drop_all, "m1", "m5"},
    {:elect, "m1", ~w[m2 m3]},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:exchange, "m1", "m3"},
    {:expect_holders, 2, 1, 3},
    {:crash, "m1"},
    {:restart, "m5"},
    {:campaign, "m5"},
    {:drop_all, "m5", "m1"},
    {:drop_all, "m5", "m2"},
    {:drop_all, "m5", "m3"},
    {:drop_all, "m5", "m4"},
    {:elect, "m5", ~w[m3 m4]},
    {:exchange, "m5", "m3"},
    {:exchange, "m5", "m3"},
    {:exchange, "m5", "m2"},
    {:exchange, "m5", "m2"},
    {:restart, "m1"}
  ]

  @buggy %{size: 5, max_append_entries: 1, bug: :commit_old_term}

  describe "old term entry" do
    test "is lost when replica count commits it" do
      cluster = Scenario.play(@script, @buggy)

      result = Scenario.finish(cluster)

      assert {:acknowledged_loss, 2} in result.violations
    end

    test "makes the new leader miss it when replica count commits it" do
      cluster = Scenario.play(@script, @buggy)

      result = Scenario.finish(cluster)

      assert {:leader_completeness, "m5", 2} in result.violations
    end

    test "stays uncommitted on the real core" do
      cluster = Scenario.play(@script, %{size: 5, max_append_entries: 1})

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "is overwritten on the real core after a majority held it" do
      cluster = Scenario.play(@script, %{size: 5, max_append_entries: 1})

      result = Scenario.finish(cluster)

      assert result.coverage.conflict_truncation >= 1
    end

    test "leaves the cluster live on the real core" do
      cluster = Scenario.play(@script, %{size: 5, max_append_entries: 1})

      result = Scenario.finish(cluster)

      assert result.liveness == []
    end
  end
end
