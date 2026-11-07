defmodule Keel.KV.PartitionTest do
  use Keel.KV.ClusterCase, async: false

  describe "partition!/2" do
    test "heals and converges after the leader was cut off", %{cluster: cluster} do
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)

      :ok = TestCluster.partition!(cluster, [leader])
      :ok = TestCluster.wait_for(cluster, &TestCluster.leader_changed?(&1, leader), 15_000)

      {:ok, revision} = KV.put(cluster.group, "during", "split", timeout: 20_000)

      :ok = TestCluster.heal!(cluster)
      converged = TestCluster.wait_for(cluster, &TestCluster.converged?/1, 30_000)
      found = KV.get(cluster.group, "during")

      assert converged == :ok
      assert {:ok, %{value: "split", revision: ^revision}} = found
    end

    test "keeps writes made before the split", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "early", "write")
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)

      :ok = TestCluster.partition!(cluster, [leader])
      :ok = TestCluster.wait_for(cluster, &TestCluster.leader_changed?(&1, leader), 15_000)
      :ok = TestCluster.heal!(cluster)
      :ok = TestCluster.wait_for(cluster, &TestCluster.converged?/1, 30_000)

      found = KV.get(cluster.group, "early", timeout: 15_000)

      assert {:ok, %{value: "write"}} = found
    end
  end
end
