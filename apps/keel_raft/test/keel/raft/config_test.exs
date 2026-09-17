defmodule Keel.Raft.ConfigTest do
  use ExUnit.Case, async: true

  alias Keel.Raft.Config
  alias Keel.Raft.Log
  alias Keel.TestUtils

  describe "apply_change/2" do
    test "fails to add a voter as a learner" do
      config = TestUtils.random_config()

      changed = Config.apply_change(config, {:add_learner, "m2"})

      assert changed == {:error, :already_member}
    end

    test "fails to add a learner twice" do
      config = TestUtils.random_config(learners: ["m4"])

      changed = Config.apply_change(config, {:add_learner, "m4"})

      assert changed == {:error, :already_member}
    end

    test "fails to add a removed id" do
      config = TestUtils.random_config(removed: ["m9"])

      changed = Config.apply_change(config, {:add_learner, "m9"})

      assert changed == {:error, :member_removed}
    end

    test "fails to promote an id that is not a member" do
      config = TestUtils.random_config()

      changed = Config.apply_change(config, {:promote, "m9"})

      assert changed == {:error, :not_member}
    end

    test "fails to promote a voter" do
      config = TestUtils.random_config()

      changed = Config.apply_change(config, {:promote, "m1"})

      assert changed == {:error, :already_member}
    end

    test "fails to remove an id that is not a member" do
      config = TestUtils.random_config()

      changed = Config.apply_change(config, {:remove, "m9"})

      assert changed == {:error, :not_member}
    end

    test "fails to remove the last voter" do
      config = TestUtils.random_config(voters: ["m1"], learners: ["m2"])

      changed = Config.apply_change(config, {:remove, "m1"})

      assert changed == {:error, :last_voter}
    end

    test "adds a learner with its address" do
      config = TestUtils.random_config()

      {:ok, changed} = Config.apply_change(config, {:add_learner, "m4", "peer@host"})
      learners = Config.learners(changed)

      assert learners == ["m4"]
      assert changed.addresses == %{"m4" => "peer@host"}
    end

    test "moves a learner to the voters" do
      config = TestUtils.random_config(learners: ["m4"])

      {:ok, changed} = Config.apply_change(config, {:promote, "m4"})
      voters = Config.voters(changed)

      assert voters == ~w[m1 m2 m3 m4]
      assert changed.learners == MapSet.new()
    end

    test "records a removed voter" do
      config = TestUtils.random_config()

      {:ok, changed} = Config.apply_change(config, {:remove, "m3"})
      removed = Config.removed(changed)

      assert changed.voters == MapSet.new(~w[m1 m2])
      assert removed == ["m3"]
    end

    test "records a removed learner" do
      config = TestUtils.random_config(learners: ["m4"], addresses: %{"m4" => "peer@host"})

      {:ok, changed} = Config.apply_change(config, {:remove, "m4"})
      removed = Config.removed(changed)

      assert removed == ["m4"]
      assert changed.addresses == %{}
    end
  end

  describe "decode/1" do
    test "returns an error for bytes that are not a config" do
      decoded = Config.decode(<<1, 2, 3>>)

      assert decoded == :error
    end

    test "returns an error for another term" do
      bytes = :erlang.term_to_binary(:other)

      decoded = Config.decode(bytes)

      assert decoded == :error
    end

    test "returns what encode wrote" do
      config =
        TestUtils.random_config(learners: ["m4"], removed: ["m9"], addresses: %{"m4" => "a@b"})

      bytes = Config.encode(config)

      decoded = Config.decode(bytes)

      assert decoded == {:ok, config}
    end
  end

  describe "quorum/1" do
    test "counts the voters only" do
      config = TestUtils.random_config(voters: ~w[m1 m2 m3 m4], learners: ~w[m5 m6 m7])

      quorum = Config.quorum(config)

      assert quorum == 3
    end
  end

  describe "current/1" do
    test "returns the config of the newest entry" do
      older = TestUtils.random_config()
      newer = TestUtils.random_config(voters: ~w[m1 m2], learners: ["m3"])
      entries = [TestUtils.config_entry(1, 0, older), TestUtils.config_entry(2, 1, newer)]
      log = Log.new(entries)

      current = Config.current(log)

      assert current == newer
    end

    test "returns the config of the entry before a truncated one" do
      older = TestUtils.random_config()
      newer = TestUtils.random_config(voters: ~w[m1 m2], learners: ["m3"])
      entries = [TestUtils.config_entry(1, 0, older), TestUtils.config_entry(2, 1, newer)]
      log = entries |> Log.new() |> Log.truncate_from(2)

      current = Config.current(log)

      assert current == older
    end

    test "returns the base config of a log with no entry" do
      base = TestUtils.random_config()
      log = [] |> Log.new() |> Log.put_base_config(base)

      current = Config.current(log)

      assert current == base
    end
  end
end
