defmodule Keel.KV.ClusterTest do
  use Keel.KV.ClusterCase, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Keel.Kv
  alias Mix.Tasks.Keel.Status

  describe "start!/1" do
    test "elects one leader", %{cluster: cluster} do
      statuses = TestCluster.status(cluster)

      leader? = TestCluster.leader?(statuses)

      assert leader?
    end
  end

  describe "facade" do
    test "returns the current revision on a stale compare and set", %{cluster: cluster} do
      {:ok, revision} = KV.put(cluster.group, "stale", "one")

      stale = KV.compare_and_set(cluster.group, "stale", revision - 1, {:put, "two"})

      assert {:error, {:revision_mismatch, ^revision}} = stale
    end

    test "returns nil for a missing key", %{cluster: cluster} do
      found = KV.get(cluster.group, "missing")

      assert found == {:ok, nil}
    end

    test "writes and reads a key", %{cluster: cluster} do
      {:ok, revision} = KV.put(cluster.group, "name", "keel")

      found = KV.get(cluster.group, "name")

      assert {:ok, %{value: "keel", revision: ^revision}} = found
    end

    test "deletes a key", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "gone", "soon")
      {:ok, _revision} = KV.delete(cluster.group, "gone")

      found = KV.get(cluster.group, "gone")

      assert found == {:ok, nil}
    end

    test "scans the keys under a prefix", %{cluster: cluster} do
      {:ok, _revision} = KV.put(cluster.group, "p/1", "a")
      {:ok, _revision} = KV.put(cluster.group, "p/2", "b")
      {:ok, _revision} = KV.put(cluster.group, "q/1", "c")

      page = KV.prefix(cluster.group, "p/", nil, 10)

      assert {:ok, %{records: [%{key: "p/1"}, %{key: "p/2"}], next: nil}} = page
    end
  end

  describe "mix keel.kv" do
    test "round trips a key given as hex", %{cluster: cluster} do
      member = TestCluster.node_of(cluster, "a")
      node = Atom.to_string(member)
      group = Atom.to_string(cluster.group)
      put = ["put", "6b6579", "--hex", "--value", "hello", "--node", node, "--group", group]
      get = ["get", "6b6579", "--hex", "--show", "--node", node, "--group", group]

      capture_io(fn -> Kv.run(put) end)
      output = capture_io(fn -> Kv.run(get) end)

      assert output =~ "hello"
    end
  end

  describe "mix keel.status" do
    test "prints a line for every member", %{cluster: cluster} do
      member = TestCluster.node_of(cluster, "a")
      node = Atom.to_string(member)
      args = [Atom.to_string(cluster.group), "--node", node]

      output = capture_io(fn -> Status.run(args) end)

      assert output =~ ~r/^a .*\nb .*\nc /
    end
  end
end
