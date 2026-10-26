defmodule KeelSim.Regression.ChangeWithoutTermCommitTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # Four voters and a learner. The first leader promotes the learner and the entry reaches
  # the learner only. A second leader has not committed an entry of its own term yet and
  # removes the first member. Two members commit that change. The first member returns and
  # wins with the learner and a member that never saw the change.
  @script [
    {:join, "m5"},
    {:elect, "m1", ~w[m2 m3]},
    {:sync, "m1"},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:exchange, "m1", "m4"},
    {:change, "m1", {:add_learner, "m5"}},
    {:sync, "m1"},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:exchange, "m1", "m4"},
    {:tick, "m1", 2},
    {:exchange, "m1", "m2"},
    {:exchange, "m1", "m3"},
    {:exchange, "m1", "m4"},
    {:tick, "m1", 2},
    {:exchange, "m1", "m5"},
    {:tick, "m1", 2},
    {:exchange, "m1", "m5"},
    {:tick, "m1", 2},
    {:exchange, "m1", "m5"},
    {:tick, "m1", 2},
    {:exchange, "m1", "m5"},
    {:change, "m1", {:promote, "m5"}},
    {:sync, "m1"},
    {:drop_all, "m1", "m2"},
    {:drop_all, "m1", "m3"},
    {:drop_all, "m1", "m4"},
    {:exchange, "m1", "m5"},
    {:exchange, "m1", "m5"},
    {:drop_all, "m1", "m5"},
    {:crash, "m1"},
    {:elect, "m2", ~w[m3 m4]},
    {:change, "m2", {:remove, "m1"}},
    {:sync, "m2"},
    {:drop_all, "m2", "m4"},
    {:drop_all, "m2", "m5"},
    {:drop_all, "m2", "m1"},
    {:exchange, "m2", "m3"},
    {:tick, "m2", 2},
    {:exchange, "m2", "m3"},
    {:tick, "m2", 2},
    {:exchange, "m2", "m3"},
    {:tick, "m2", 2},
    {:exchange, "m2", "m3"},
    {:crash, "m2"},
    {:crash, "m3"},
    {:restart, "m1"},
    {:campaign, "m1"},
    {:elect, "m1", ~w[m4 m5]}
  ]

  @opts %{size: 4, max_append_entries: 1, stop_removed: false}

  describe "a change before the leader commits in its own term" do
    test "loses a committed entry when it is allowed" do
      opts = Map.put(@opts, :bug, :change_without_term_commit)
      cluster = Scenario.play(@script, opts)

      result = Scenario.finish(cluster)

      assert {:leader_completeness, "m1", 3} in result.violations
    end

    test "is refused on the real core" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.coverage.change_before_term_commit_refused == 1
    end

    test "leaves every check clean on the real core" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "leaves the cluster live on the real core" do
      cluster = Scenario.play(@script, @opts)

      result = Scenario.finish(cluster)

      assert result.liveness == []
    end
  end
end
