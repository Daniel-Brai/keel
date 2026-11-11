defmodule Keel.KV.LazyFsTest do
  use ExUnit.Case, async: false

  alias Keel.KV.TestCluster
  alias Keel.KV.TestUtils

  @moduletag :lazyfs
  @moduletag :tmp_dir
  @moduletag timeout: 3_600_000
  @moduletag skip: TestUtils.lazyfs_skip()

  setup %{tmp_dir: dir} do
    {:ok, TestUtils.start_lazyfs_cluster!(dir)}
  end

  describe "power_cycle_loop!/4" do
    test "keeps every acknowledged write across a power cycle every 10 s (T-28.6)", %{
      cluster: cluster,
      mounts: mounts
    } do
      deadline = System.monotonic_time(:millisecond) + TestUtils.power_seconds() * 1_000
      writes = TestCluster.start_writes_until!(cluster, deadline)

      cycled = TestUtils.power_cycle_loop!(cluster, mounts, 10_000, deadline)
      results = Task.await(writes, 120_000)
      converged = TestCluster.wait_for(cycled, &TestCluster.converged?/1, 60_000)
      lost = TestCluster.lost(results, cycled)

      assert converged == :ok
      assert lost == []
    end
  end

  describe "power_cycle_all!/2" do
    test "keeps every acknowledged write when every member loses its cache (T-28.7)", %{
      cluster: cluster,
      mounts: mounts
    } do
      writes = TestCluster.start_writes!(cluster, 300, self())
      results = Task.await(writes, 120_000)

      restarted = TestUtils.power_cycle_all!(cluster, mounts)
      lost = TestCluster.lost(results, restarted)

      assert lost == []
    end
  end

  describe "tear_last_record!/1" do
    test "truncates the torn record and lets the member rejoin (T-28.8)", %{
      cluster: cluster,
      mounts: mounts
    } do
      statuses = TestCluster.status(cluster)
      follower = TestCluster.follower_of(statuses)
      mount = Map.fetch!(mounts, follower)
      writes = TestCluster.start_writes!(cluster, 50, self())
      results = Task.await(writes, 60_000)

      dead = TestUtils.power_down!(cluster, mount, follower)
      used = TestUtils.tear_last_record!(mount)
      restarted = TestCluster.restart!(dead, follower)
      converged = TestCluster.wait_for(restarted, &TestCluster.converged?/1, 60_000)
      lost = TestCluster.lost(results, restarted)

      assert used > 4
      assert converged == :ok
      assert lost == []
    end
  end
end
