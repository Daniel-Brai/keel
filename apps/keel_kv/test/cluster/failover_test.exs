defmodule Keel.KV.FailoverTest do
  use Keel.KV.ClusterCase, async: false

  describe "kill!/2" do
    test "keeps every acknowledged write when the leader dies", %{cluster: cluster} do
      writes = TestCluster.start_writes!(cluster, 40, self())

      assert_receive {:acked, _key}
      assert_receive {:acked, _key}
      assert_receive {:acked, _key}

      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)
      degraded = TestCluster.kill!(cluster, leader)
      results = Task.await(writes, 60_000)
      changed = TestCluster.wait_for(degraded, &TestCluster.leader_changed?(&1, leader), 15_000)
      lost = TestCluster.lost(results, degraded)

      assert changed == :ok
      assert lost == []
    end

    test "lets the old leader catch up after it restarts", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "before", "kill")
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)
      degraded = TestCluster.kill!(cluster, leader)
      :ok = TestCluster.wait_for(degraded, &TestCluster.leader_changed?(&1, leader), 15_000)
      {:ok, _revision} = KV.put(cluster.group, "after", "kill", timeout: 15_000)

      restored = TestCluster.restart!(degraded, leader)
      converged = TestCluster.wait_for(restored, &TestCluster.converged?/1, 30_000)
      found = KV.get(restored.group, "after", timeout: 15_000)

      assert converged == :ok
      assert {:ok, %{value: "kill"}} = found
    end
  end
end
