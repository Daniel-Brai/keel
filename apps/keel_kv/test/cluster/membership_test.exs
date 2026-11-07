defmodule Keel.KV.MembershipTest do
  use Keel.KV.ClusterCase, async: false

  describe "add_member/4" do
    test "grows from three to five members under writes", %{cluster: cluster} do
      writes = TestCluster.start_writes!(cluster, 60, self())

      assert_receive {:acked, _key}

      with_d = TestCluster.join!(cluster, "d")
      added_d = TestCluster.add_member!(with_d, "a", "d", :voter)
      with_e = TestCluster.join!(with_d, "e")
      added_e = TestCluster.add_member!(with_e, "a", "e", :voter)
      results = Task.await(writes, 90_000)
      grown = TestCluster.wait_for(with_e, &TestCluster.voted?(&1, ~w[a b c d e]), 30_000)
      converged = TestCluster.wait_for(with_e, &TestCluster.converged?/1, 30_000)
      lost = TestCluster.lost(results, with_e)

      assert {added_d, added_e, grown, converged} == {:ok, :ok, :ok, :ok}
      assert lost == []
    end

    test "catches a learner up through a snapshot before promoting it", %{cluster: cluster} do
      TestCluster.put_many!(cluster, 80)
      :ok = TestCluster.wait_for(cluster, &TestCluster.compacted_past?(&1, 1), 15_000)

      with_d = TestCluster.join!(cluster, "d")
      added = TestCluster.add_member!(with_d, "a", "d", :learner)
      caught = TestCluster.wait_for(with_d, &TestCluster.caught_up?(&1, "d"), 30_000)
      behind = TestCluster.status(with_d)
      promoted = TestCluster.promote_member!(with_d, "a", "d")
      voted = TestCluster.wait_for(with_d, &TestCluster.voted?(&1, ~w[a b c d]), 30_000)

      assert {added, caught, promoted, voted} == {:ok, :ok, :ok, :ok}
      assert TestCluster.snapshotted?(behind, "d")
    end
  end

  describe "remove_member/2" do
    test "removes a follower and goes on", %{cluster: cluster} do
      statuses = TestCluster.status(cluster)
      follower = TestCluster.follower_of(statuses)
      voters = Enum.reject(~w[a b c], &(&1 == follower))

      removed = TestCluster.remove_member!(cluster, "a", follower)
      shrunk = TestCluster.drop!(cluster, follower)
      settled = TestCluster.wait_for(shrunk, &TestCluster.voted?(&1, voters), 30_000)
      written = KV.put(shrunk.group, "after", "removal", timeout: 15_000)

      assert {removed, settled} == {:ok, :ok}
      assert {:ok, _revision} = written
    end

    test "removes the leader which steps down while the rest go on", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "before", "removal")
      statuses = TestCluster.status(cluster)
      leader = TestCluster.leader_of(statuses)
      voters = Enum.reject(~w[a b c], &(&1 == leader))

      removed = TestCluster.remove_member!(cluster, leader, leader)
      moved = TestCluster.wait_for(cluster, &TestCluster.leader_changed?(&1, leader), 30_000)
      after_removal = TestCluster.status(cluster)
      shrunk = TestCluster.drop!(cluster, leader)
      settled = TestCluster.wait_for(shrunk, &TestCluster.voted?(&1, voters), 30_000)
      written = KV.put(shrunk.group, "after", "removal", timeout: 15_000)
      kept = KV.get(shrunk.group, "before", timeout: 15_000)

      assert {removed, moved, settled} == {:ok, :ok, :ok}
      assert {:ok, _revision} = written
      assert {:ok, %{value: "removal"}} = kept
      assert TestCluster.voters_of(after_removal, leader) == voters
    end
  end
end
