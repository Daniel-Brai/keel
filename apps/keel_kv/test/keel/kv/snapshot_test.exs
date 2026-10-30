defmodule Keel.KV.SnapshotTest do
  use ExUnit.Case, async: true

  alias Keel.KV.Machine
  alias Keel.KV.Snapshot
  alias Keel.KV.TestUtils

  setup do
    keys = TestUtils.keys(60)
    {store, _client} = TestUtils.populated(60, keys)
    sessions = TestUtils.sessions(store, 40, 100)

    %{store: sessions}
  end

  describe "decode/1" do
    test "fails on empty bytes" do
      decoded = Snapshot.decode(<<>>)

      assert {:error, :corrupt_snapshot} = decoded
    end

    test "fails on bytes after the snapshot", %{store: store} do
      bytes = Snapshot.encode(store)

      decoded = Snapshot.decode(bytes <> "x")

      assert {:error, :corrupt_snapshot} = decoded
    end

    test "fails on every cut of a snapshot" do
      {store, _client} = TestUtils.populated(3, TestUtils.keys(3))
      bytes = Snapshot.encode(store)

      cuts = TestUtils.restoring_prefixes(bytes)

      assert cuts == []
    end

    test "fails when any single bit flips" do
      {store, client} = TestUtils.populated(3, TestUtils.keys(3))

      {used, _result} =
        TestUtils.run(
          store,
          {10, 10},
          {client, 9, 9},
          {:compare_and_set, "key-0001", 2, {:put, "v"}}
        )

      bytes = Snapshot.encode(used)

      flips = TestUtils.undetected_flips(bytes)

      assert flips == []
    end

    test "returns the same bytes after a restore", %{store: store} do
      bytes = Snapshot.encode(store)
      {:ok, restored} = Snapshot.decode(bytes)

      encoded = Snapshot.encode(restored)

      assert encoded == bytes
    end

    test "returns the same records and sessions", %{store: store} do
      {:ok, restored} = store |> Machine.snapshot() |> Machine.restore()
      fetched = Machine.fetch(restored, "key-0007")
      ids = TestUtils.session_ids(restored)

      expected_value = Machine.fetch(store, "key-0007")
      expected_ids = TestUtils.session_ids(store)

      assert {fetched, ids} == {expected_value, expected_ids}
    end
  end
end
