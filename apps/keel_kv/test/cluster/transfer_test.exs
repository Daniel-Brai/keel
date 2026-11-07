defmodule Keel.KV.TransferTest do
  use Keel.KV.ClusterCase, async: false

  describe "transfer_leadership/2" do
    test "fails on a member that is not the leader", %{cluster: cluster} do
      statuses = TestCluster.status(cluster)
      follower = TestCluster.follower_of(statuses)

      reply = TestCluster.transfer!(cluster, follower, "a")

      assert {:error, {:not_leader, _leader}} = reply
    end

    test "moves leadership to the target", %{cluster: cluster} do
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)
      target = TestCluster.follower_of(statuses)

      reply = TestCluster.transfer!(cluster, leader, target)
      moved = TestCluster.wait_for(cluster, &TestCluster.led_by?(&1, target), 15_000)

      assert {reply, moved} == {:ok, :ok}
    end

    test "keeps every acknowledged write under load", %{cluster: cluster} do
      writes = TestCluster.start_writes!(cluster, 40, self())

      assert_receive {:acked, _key}
      assert_receive {:acked, _key}

      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)
      target = TestCluster.follower_of(statuses)
      _reply = TestCluster.transfer!(cluster, leader, target)
      results = Task.await(writes, 60_000)
      changed = TestCluster.wait_for(cluster, &TestCluster.leader_changed?(&1, leader), 15_000)
      lost = TestCluster.lost(results, cluster)

      assert changed == :ok
      assert lost == []
    end
  end
end
