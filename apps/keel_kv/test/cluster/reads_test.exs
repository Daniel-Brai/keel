defmodule Keel.KV.ReadsTest do
  use Keel.KV.ClusterCase, async: false

  describe "get/3" do
    test "returns the latest write after the leader dies", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "latest", "one")
      {:ok, revision} = KV.put(cluster.group, "latest", "two")
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)
      degraded = TestCluster.kill!(cluster, leader)
      :ok = TestCluster.wait_for(degraded, &TestCluster.leader_changed?(&1, leader), 15_000)

      found = KV.get(degraded.group, "latest", timeout: 15_000)

      assert {:ok, %{value: "two", revision: ^revision}} = found
    end

    test "reads on a follower what the leader committed", %{cluster: cluster} do
      {:ok, revision} = KV.put(cluster.group, "spread", "value")
      statuses = TestCluster.status(cluster)
      follower = TestCluster.follower_of(statuses)

      reply = TestCluster.read_at(cluster, follower, {:get, "spread"})

      assert {:read, {:ok, {"value", ^revision}}} = reply
    end

    test "answers through the log when asked to", %{cluster: cluster} do
      {:ok, revision} = KV.put(cluster.group, "logged", "value")

      found = KV.get(cluster.group, "logged", read: :log)

      assert {:ok, %{value: "value", revision: ^revision}} = found
    end

    test "refuses a read on the leader that a partition cut off", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "stale", "old")
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)

      :ok = TestCluster.partition!(cluster, [leader])
      reply = TestCluster.read_at(cluster, leader, {:get, "stale"})

      assert {:refused, _reason} = reply
    end

    test "returns the new value after a partition moved the leader", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "moved", "old")
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)

      :ok = TestCluster.partition!(cluster, [leader])
      :ok = TestCluster.wait_for(cluster, &TestCluster.leader_changed?(&1, leader), 15_000)
      {:ok, revision} = KV.put(cluster.group, "moved", "new", timeout: 20_000)

      found = KV.get(cluster.group, "moved", timeout: 15_000)

      assert {:ok, %{value: "new", revision: ^revision}} = found
    end
  end
end
