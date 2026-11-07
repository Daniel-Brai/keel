defmodule Keel.KV.SnapshotInstallTest do
  use Keel.KV.ClusterCase, async: false

  describe "restart!/2" do
    test "catches a follower up through a snapshot after the others compacted", %{
      cluster: cluster
    } do
      {:ok, _revision} = KV.put(cluster.group, "before", "down")
      statuses = TestCluster.status(cluster)
      follower = TestCluster.follower_of(statuses)
      behind = TestCluster.last_index_of(statuses, follower)
      degraded = TestCluster.kill!(cluster, follower)
      TestCluster.put_many!(degraded, 80)
      :ok = TestCluster.wait_for(degraded, &TestCluster.compacted_past?(&1, behind), 15_000)

      restored = TestCluster.restart!(degraded, follower)
      converged = TestCluster.wait_for(restored, &TestCluster.converged?/1, 30_000)
      caught_up = TestCluster.status(restored)

      assert converged == :ok
      assert TestCluster.first_index_of(caught_up, follower) > behind + 1
    end

    test "keeps every write after the whole cluster restarts from snapshots", %{cluster: cluster} do
      TestCluster.put_many!(cluster, 60)
      statuses = TestCluster.status(cluster)
      :ok = TestCluster.wait_for(cluster, &TestCluster.compacted_past?(&1, 1), 15_000)
      leader = TestCluster.leader_of(statuses)
      degraded = TestCluster.kill!(cluster, leader)
      :ok = TestCluster.wait_for(degraded, &TestCluster.leader_changed?(&1, leader), 15_000)

      restored = TestCluster.restart!(degraded, leader)
      :ok = TestCluster.wait_for(restored, &TestCluster.converged?/1, 30_000)
      found = KV.get(restored.group, "fill-60", timeout: 15_000)

      assert {:ok, %{value: value}} = found
      assert byte_size(value) == 60
    end
  end
end
