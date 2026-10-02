defmodule KeelSim.InvariantsTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.Log
  alias KeelSim.Invariants
  alias KeelSim.TestUtils

  describe "term_stepped/4" do
    test "reports a term that went down" do
      checked = Invariants.term_stepped(%Invariants{}, "m1", 5, 3)

      assert checked.violations == [{:term_regressed, "m1", 5, 3}]
    end

    test "accepts a term that stayed" do
      checked = Invariants.term_stepped(%Invariants{}, "m1", 5, 5)

      assert checked.violations == []
    end

    test "accepts a term that rose" do
      checked = Invariants.term_stepped(%Invariants{}, "m1", 5, 6)

      assert checked.violations == []
    end
  end

  describe "read_served/4" do
    test "reports a read that began after a newer leader committed" do
      checked = Invariants.read_served(%Invariants{}, "m1", 1, 2)

      assert checked.violations == [{:stale_leader_read, "m1", 1}]
    end

    test "accepts a read that began before any newer commit" do
      checked = Invariants.read_served(%Invariants{}, "m1", 2, 2)

      assert checked.violations == []
    end
  end

  describe "committed_in/3" do
    test "keeps the newest term that committed" do
      noted = Invariants.committed_in(%Invariants{}, 1, 3)

      checked = Invariants.committed_in(noted, 2, 2)

      assert checked.newest_commit_term == 3
    end
  end

  describe "applied/3" do
    test "reports log time that went down" do
      first = %Entry{term: 1, index: 1, kind: :command, data: "a", time_ms: 500}
      second = %Entry{term: 1, index: 2, kind: :command, data: "b", time_ms: 400}

      applied = Invariants.applied(%Invariants{}, "m1", first)
      checked = Invariants.applied(applied, "m1", second)

      assert checked.violations == [{:time_regressed, "m1", 2}]
    end

    test "reports an entry that carries another time on another member" do
      entry = %Entry{term: 1, index: 1, kind: :command, data: "a", time_ms: 500}
      other = %Entry{entry | time_ms: 501}

      applied = Invariants.applied(%Invariants{}, "m1", entry)
      checked = Invariants.applied(applied, "m2", other)

      assert checked.violations == [{:time_diverged, "m2", 1}]
    end

    test "accepts time that stayed or rose" do
      first = %Entry{term: 1, index: 1, kind: :command, data: "a", time_ms: 500}
      second = %Entry{term: 1, index: 2, kind: :command, data: "b", time_ms: 500}
      third = %Entry{term: 1, index: 3, kind: :command, data: "c", time_ms: 900}

      checked =
        Enum.reduce([first, second, third], %Invariants{}, &Invariants.applied(&2, "m1", &1))

      assert checked.violations == []
    end
  end

  describe "result_applied/4" do
    test "reports another result for the same index" do
      applied = Invariants.result_applied(%Invariants{}, "m1", 4, {:ok, 4})

      checked = Invariants.result_applied(applied, "m2", 4, {:ok, 0})

      assert checked.violations == [{:result_diverged, "m2", 4}]
    end

    test "accepts the same result for the same index" do
      applied = Invariants.result_applied(%Invariants{}, "m1", 4, {:ok, 4})

      checked = Invariants.result_applied(applied, "m2", 4, {:ok, 4})

      assert checked.violations == []
    end
  end

  describe "state_applied/4" do
    test "reports another state at the same index" do
      applied = Invariants.state_applied(%Invariants{}, "m1", 16, "one")

      checked = Invariants.state_applied(applied, "m2", 16, "two")

      assert checked.violations == [{:state_diverged, "m2", 16}]
    end
  end

  describe "leader_log/4" do
    test "reports a leader that lacks an entry committed in a lower term" do
      invariants = %Invariants{committed: %{1 => {2, "a"}}, commit_terms: %{1 => 2}}
      log = Log.new([])

      checked = Invariants.leader_log(invariants, "m1", 3, log)

      assert checked.violations == [{:leader_completeness, "m1", 1}]
    end

    test "accepts a late leader of an older term than the commit" do
      invariants = %Invariants{committed: %{1 => {5, "a"}}, commit_terms: %{1 => 5}}
      log = Log.new([])

      checked = Invariants.leader_log(invariants, "m1", 4, log)

      assert checked.violations == []
    end

    test "accepts a leader that holds the entry" do
      invariants = %Invariants{committed: %{1 => {2, "a"}}, commit_terms: %{1 => 2}}
      entry = %Entry{term: 2, index: 1, kind: :command, data: "a", time_ms: 0}

      checked = Invariants.leader_log(invariants, "m1", 3, Log.new([entry]))

      assert checked.violations == []
    end
  end

  describe "leader_log/4 with a snapshot" do
    test "accepts a leader whose snapshot covers the entry" do
      invariants = %Invariants{committed: %{1 => {2, "a"}}, commit_terms: %{1 => 2}}
      log = TestUtils.snapshotted_log(1, 2)

      checked = Invariants.leader_log(invariants, "m1", 3, log)

      assert checked.violations == []
    end
  end

  describe "snapshot_state/4" do
    test "reports a state that differs from the one held at that index" do
      held = Invariants.snapshot_state(%Invariants{}, "m1", 8, "one")

      checked = Invariants.snapshot_state(held, "m2", 8, "two")

      assert checked.violations == [{:snapshot_diverged, "m2", 8}]
    end

    test "accepts the same state at that index" do
      held = Invariants.snapshot_state(%Invariants{}, "m1", 8, "one")

      checked = Invariants.snapshot_state(held, "m2", 8, "one")

      assert checked.violations == []
    end
  end

  describe "raised/2" do
    test "reports the invariant that the core raised" do
      checked = Invariants.raised(%Invariants{}, :truncate_committed)

      assert checked.violations == [{:raised, :truncate_committed}]
    end
  end

  describe "acknowledged/3" do
    test "reports an entry that a majority lacks" do
      invariants = %Invariants{committed: %{1 => {1, "a"}}}
      logs = %{"m1" => {:ok, %{1 => {1, "a"}}}, "m2" => {:ok, %{}}, "m3" => {:ok, %{}}}

      checked = Invariants.acknowledged(invariants, logs, 2)

      assert checked.violations == [{:acknowledged_loss, 1}]
    end

    test "reports an entry that a majority holds with another term" do
      invariants = %Invariants{committed: %{1 => {2, "a"}}}

      logs = %{
        "m1" => {:ok, %{1 => {2, "a"}}},
        "m2" => {:ok, %{1 => {1, "a"}}},
        "m3" => {:ok, %{}}
      }

      checked = Invariants.acknowledged(invariants, logs, 2)

      assert checked.violations == [{:acknowledged_loss, 1}]
    end

    test "reports a log that cannot be recovered" do
      logs = %{"m1" => {:ok, %{}}, "m2" => {:error, :broken}}

      checked = Invariants.acknowledged(%Invariants{}, logs, 2)

      assert checked.violations == [{:recovery_failed, "m2", :broken}]
    end

    test "accepts an entry that a majority holds" do
      invariants = %Invariants{committed: %{1 => {1, "a"}}}

      logs = %{
        "m1" => {:ok, %{1 => {1, "a"}}},
        "m2" => {:ok, %{1 => {1, "a"}}},
        "m3" => {:ok, %{}}
      }

      checked = Invariants.acknowledged(invariants, logs, 2)

      assert checked.violations == []
    end
  end

  describe "acknowledged/3 with a recorded config" do
    test "reports an entry that the voters of its config lack" do
      config = TestUtils.random_config(voters: ~w[m1 m2 m3])
      committed = Invariants.committed_with(%Invariants{committed: %{1 => {1, "a"}}}, 1, config)

      logs = %{
        "m1" => {:ok, %{1 => {1, "a"}}},
        "m2" => {:ok, %{}},
        "m4" => {:ok, %{1 => {1, "a"}}}
      }

      checked = Invariants.acknowledged(committed, logs, 1)

      assert checked.violations == [{:acknowledged_loss, 1}]
    end

    test "accepts an entry that the voters of its config hold" do
      config = TestUtils.random_config(voters: ~w[m1 m2 m3])
      committed = Invariants.committed_with(%Invariants{committed: %{1 => {1, "a"}}}, 1, config)

      logs = %{
        "m1" => {:ok, %{1 => {1, "a"}}},
        "m2" => {:ok, %{1 => {1, "a"}}},
        "m3" => {:ok, %{}},
        "m4" => {:ok, %{}},
        "m5" => {:ok, %{}}
      }

      checked = Invariants.acknowledged(committed, logs, 3)

      assert checked.violations == []
    end
  end

  describe "committed_with/3" do
    test "keeps the config of the first member to apply an entry" do
      first = TestUtils.random_config(voters: ~w[m1 m2 m3])
      later = TestUtils.random_config(voters: ["m1"])
      invariants = Invariants.committed_with(%Invariants{}, 1, first)

      kept = Invariants.committed_with(invariants, 1, later)

      assert kept.commit_configs == %{1 => {first.voters, 2}}
    end
  end

  describe "config_applied/3" do
    test "reports an id that rejoins after its removal" do
      removed = TestUtils.random_config(voters: ~w[m1 m2], removed: ["m3"])
      rejoined = TestUtils.random_config(voters: ~w[m1 m2], learners: ["m3"], removed: ["m3"])

      first =
        Invariants.config_applied(%Invariants{}, "m1", TestUtils.config_entry(2, 1, removed))

      checked = Invariants.config_applied(first, "m1", TestUtils.config_entry(3, 1, rejoined))

      assert checked.violations == [{:removed_rejoined, "m3"}]
    end

    test "counts an entry once however many members apply it" do
      removed = TestUtils.random_config(voters: ~w[m1 m2], removed: ["m3"])
      older = TestUtils.random_config(voters: ~w[m1 m2 m3])
      committed = Invariants.committed_with(%Invariants{}, 1, older)
      first = Invariants.config_applied(committed, "m1", TestUtils.config_entry(2, 1, removed))
      held = Invariants.committed_with(first, 2, removed)

      checked = Invariants.config_applied(held, "m2", TestUtils.config_entry(1, 0, older))

      assert checked.violations == []
    end

    test "records the removed ids" do
      removed = TestUtils.random_config(voters: ~w[m1 m2], removed: ["m3"])

      checked =
        Invariants.config_applied(%Invariants{}, "m1", TestUtils.config_entry(2, 1, removed))

      assert checked.removed == MapSet.new(["m3"])
    end
  end

  describe "vote_recovered/3" do
    test "reports a vote that was forgotten" do
      invariants = %Invariants{durable_votes: %{{"m1", 3} => "m2"}}
      recovered = %HardState{term: 2, voted_for: nil, commit: 0}

      checked = Invariants.vote_recovered(invariants, "m1", recovered)

      assert checked.violations == [{:vote_lost, "m1", 3}]
    end

    test "reports a vote that changed" do
      invariants = %Invariants{durable_votes: %{{"m1", 3} => "m2"}}
      recovered = %HardState{term: 3, voted_for: "m3", commit: 0}

      checked = Invariants.vote_recovered(invariants, "m1", recovered)

      assert checked.violations == [{:vote_lost, "m1", 3}]
    end

    test "accepts a vote that survived" do
      invariants = %Invariants{durable_votes: %{{"m1", 3} => "m2"}}
      recovered = %HardState{term: 3, voted_for: "m2", commit: 0}

      checked = Invariants.vote_recovered(invariants, "m1", recovered)

      assert checked.violations == []
    end

    test "accepts a later term" do
      invariants = %Invariants{durable_votes: %{{"m1", 3} => "m2"}}
      recovered = %HardState{term: 4, voted_for: nil, commit: 0}

      checked = Invariants.vote_recovered(invariants, "m1", recovered)

      assert checked.violations == []
    end
  end
end
