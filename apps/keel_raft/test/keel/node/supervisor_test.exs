defmodule Keel.Node.SupervisorTest do
  use ExUnit.Case, async: true

  alias Keel.TestUtils

  alias Keel.Env.Disk.Stub, as: DiskStub
  alias Keel.Node.Supervisor, as: NodeSupervisor
  alias Keel.Transport.Stub, as: TransportStub

  @moduletag :tmp_dir
  @moduletag :capture_log

  describe "start_link/1" do
    test "fails for a new member that neither bootstraps nor joins", %{tmp_dir: dir} do
      group = TestUtils.random_group()
      TransportStub.listen(group)
      opts = TestUtils.member_opts(dir, group, bootstrap: false)

      started = NodeSupervisor.start_link(opts)

      assert started == {:error, :not_bootstrapped}
    end

    test "fails when the members it bootstraps do not name it", %{tmp_dir: dir} do
      group = TestUtils.random_group()
      TransportStub.listen(group)
      opts = TestUtils.member_opts(dir, group, members: %{"b" => node()})

      started = NodeSupervisor.start_link(opts)

      assert started == {:error, :not_bootstrapped}
    end

    test "starts from a log it wrote before", %{tmp_dir: dir} do
      group = TestUtils.start_member!(dir)
      TestUtils.await_role!(group, :leader)
      stop_supervised!(NodeSupervisor)
      opts = TestUtils.member_opts(dir, group, bootstrap: false)

      started = NodeSupervisor.start_link(opts)

      assert {:ok, _supervisor} = started
    end

    test "stops the group when the disk keeps failing", %{tmp_dir: dir} do
      group = TestUtils.random_group()
      TransportStub.listen(group)
      disk = DiskStub.new()
      opts = TestUtils.member_opts(dir, group, disk: disk)
      Process.flag(:trap_exit, true)
      {:ok, supervisor} = NodeSupervisor.start_link(opts)
      DiskStub.fail_syncs(disk)

      assert_receive {:EXIT, ^supervisor, :shutdown}
    end

    test "rebuilds the member from disk when the writer dies", %{tmp_dir: dir} do
      group = TestUtils.start_member!(dir)
      TestUtils.await_role!(group, :leader)
      {:applied, {:ok, _chain}} = TestUtils.propose(group, "x")
      before = TestUtils.status(group)
      server = TestUtils.whereis_server(group)
      writer_name = NodeSupervisor.writer_name(group)
      writer = Process.whereis(writer_name)

      Process.exit(writer, :kill)
      TestUtils.await_restart!(group, server)

      status = TestUtils.status(group)

      assert status.last_index >= before.last_index
      assert status.term >= before.term
    end

    test "replays the log into a fresh machine after a restart", %{tmp_dir: dir} do
      group = TestUtils.start_member!(dir)
      TestUtils.await_role!(group, :leader)
      {:applied, {:ok, _chain}} = TestUtils.propose(group, "x")
      server = TestUtils.whereis_server(group)
      writer_name = NodeSupervisor.writer_name(group)
      writer = Process.whereis(writer_name)

      Process.exit(writer, :kill)
      TestUtils.await_restart!(group, server)
      TestUtils.await_role!(group, :leader)

      reply = TestUtils.propose(group, "y")

      assert {:applied, {:ok, {5, _hash}}} = reply
    end
  end

  describe "snapshots" do
    test "rebuilds the machine from a snapshot after a restart", %{tmp_dir: dir} do
      raft = [snapshot_threshold_bytes: 1, snapshot_trailing_entries: 1]
      group = TestUtils.start_member!(dir, raft: raft)
      TestUtils.await_role!(group, :leader)
      {:applied, {:ok, {before, _hash}}} = TestUtils.propose(group, "x")

      TestUtils.wait_until!(fn -> TestUtils.snapshot_present?(dir) end)

      server = TestUtils.whereis_server(group)
      writer_name = NodeSupervisor.writer_name(group)
      writer = Process.whereis(writer_name)

      Process.exit(writer, :kill)
      TestUtils.await_restart!(group, server)
      TestUtils.await_role!(group, :leader)

      reply = TestUtils.propose(group, "y")

      assert {:applied, {:ok, {count, _hash}}} = reply
      assert count >= before + 2
    end
  end
end
