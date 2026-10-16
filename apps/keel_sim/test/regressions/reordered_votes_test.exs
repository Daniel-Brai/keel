defmodule KeelSim.Regression.ReorderedVotesTest do
  use ExUnit.Case, async: true

  alias KeelSim.Scenario

  # A member restarts while vote requests of two terms wait for it. The later term
  # arrives first. The earlier one arrives late and must change nothing.
  @script [
    {:campaign, "m1"},
    {:campaign, "m2"},
    {:campaign, "m2"},
    {:drop, "m2", "m3"},
    {:crash, "m3"},
    {:restart, "m3"},
    {:deliver, "m2", "m3"},
    {:sync, "m3"},
    {:deliver, "m1", "m3"}
  ]

  describe "vote requests out of order" do
    test "keeps the invariants clean across a second restart" do
      cluster = Scenario.play(@script ++ [{:crash, "m3"}, {:restart, "m3"}], %{size: 3})

      result = Scenario.finish(cluster)

      assert result.violations == []
    end

    test "never lowers the term" do
      cluster = Scenario.play(@script, %{size: 3})

      member = Scenario.node(cluster, "m3")

      assert member.term == 2
    end

    test "keeps the vote of the later term" do
      cluster = Scenario.play(@script, %{size: 3})

      member = Scenario.node(cluster, "m3")

      assert member.voted_for == "m2"
    end

    test "keeps the term and vote after a restart" do
      cluster = Scenario.play(@script ++ [{:crash, "m3"}, {:restart, "m3"}], %{size: 3})

      member = Scenario.node(cluster, "m3")

      assert {member.term, member.voted_for} == {2, "m2"}
    end

    test "reached a stale request after a restart" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.coverage.stale_vote_after_restart >= 1
    end

    test "reached a vote after a restart" do
      cluster = Scenario.play(@script, %{size: 3})

      result = Scenario.finish(cluster)

      assert result.coverage.vote_after_restart >= 1
    end
  end
end
