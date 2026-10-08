defmodule Keel.Node.ApplierTest do
  use ExUnit.Case, async: true

  alias Keel.Chain.Machine, as: ChainMachine
  alias Keel.Machine.Stub, as: MachineStub
  alias Keel.Node.Applier
  alias Keel.Raft.SnapshotMeta
  alias Keel.TestUtils

  @moduletag :tmp_dir
  @moduletag :capture_log

  describe "apply_entries/3" do
    test "stops the member when the machine raises", %{tmp_dir: dir} do
      applier = TestUtils.start_applier!(dir, MachineStub, restart: :temporary)

      [entry] = TestUtils.entries([{1, 1}])
      crash = %{entry | data: "crash"}
      ref = Process.monitor(applier)

      Applier.apply_entries(applier, [crash], %{})

      assert_receive {:DOWN, ^ref, :process, ^applier, {:shutdown, {:apply_crashed, 1}}}
    end

    test "answers a waiting caller with the machine result", %{tmp_dir: dir} do
      applier = TestUtils.start_applier!(dir, MachineStub)
      entries = TestUtils.entries([{1, 1}, {2, 1}])
      from = {self(), make_ref()}
      {_pid, tag} = from

      Applier.apply_entries(applier, entries, %{2 => from})

      assert_receive {^tag, {:applied, {:ok, 2}}}
    end

    test "does not answer entries nobody waits on", %{tmp_dir: dir} do
      applier = TestUtils.start_applier!(dir, MachineStub)
      entries = TestUtils.entries([{1, 1}])

      Applier.apply_entries(applier, entries, %{})

      refute_receive {_tag, {:applied, _result}}
    end

    test "tells the server how far it applied", %{tmp_dir: dir} do
      applier = TestUtils.start_applier!(dir, ChainMachine)
      entries = TestUtils.entries([{1, 1}, {2, 1}, {3, 1}])

      Applier.apply_entries(applier, entries, %{})

      assert_receive {:applied, 3}
    end
  end

  describe "take_snapshot/2" do
    test "tells the server which index the file holds", %{tmp_dir: dir} do
      applier = TestUtils.start_applier!(dir, ChainMachine)
      TestUtils.apply_chain!(applier, 3)

      Applier.take_snapshot(applier, 3)

      assert_receive {:snapshot_taken, %SnapshotMeta{index: 3, term: 1}}
    end

    test "carries the config of the newest config entry", %{tmp_dir: dir} do
      applier = TestUtils.start_applier!(dir, ChainMachine)
      config = TestUtils.random_config(learners: ["m4"])
      TestUtils.apply_config!(applier, config)

      meta = TestUtils.taken_snapshot!(applier)

      assert meta.config == config
    end

    test "starts the next member from the newest snapshot", %{tmp_dir: dir} do
      first = TestUtils.start_applier!(dir, ChainMachine)
      TestUtils.apply_chain!(first, 3)
      TestUtils.taken_snapshot!(first)
      second = TestUtils.start_applier!(dir, ChainMachine)
      from = {self(), make_ref()}
      {_pid, tag} = from
      [entry] = TestUtils.entries([{4, 1}])

      Applier.apply_entries(second, [entry], %{4 => from})

      assert_receive {^tag, {:applied, {:ok, {4, _hash}}}}
    end
  end

  describe "install_snapshot/2" do
    test "replaces the state and counts on from the snapshot index", %{tmp_dir: dir} do
      target = TestUtils.start_applier!(dir, ChainMachine)
      source = TestUtils.start_applier!(dir, ChainMachine)
      TestUtils.apply_chain!(source, 5)
      meta = TestUtils.taken_snapshot!(source)
      from = {self(), make_ref()}
      {_pid, tag} = from
      [entry] = TestUtils.entries([{6, 1}])

      Applier.install_snapshot(target, meta)
      Applier.apply_entries(target, [entry], %{6 => from})

      assert_receive {^tag, {:applied, {:ok, {6, _hash}}}}
    end
  end
end
