defmodule KeelSim.DiskTest do
  use ExUnit.Case, async: true

  alias KeelSim.Disk
  alias KeelSim.TestUtils

  describe "open/3" do
    test "fails without a parent directory" do
      disk = Disk.new()

      opened = Disk.open(disk, "/data/file", [:write, :create])

      assert {:error, %Disk{}, :enoent} = opened
    end

    test "fails to open a missing file" do
      {:ok, disk} = Disk.create_dir(Disk.new(), "/data")

      opened = Disk.open(disk, "/data/file", [:write])

      assert {:error, %Disk{}, :enoent} = opened
    end

    test "fails to create a file that exists with exclusive" do
      {disk, _handle} = TestUtils.disk_with_file!(:strict, 1)

      opened = Disk.open(disk, "/data/file", ~w[write create exclusive]a)

      assert {:error, %Disk{}, :eexist} = opened
    end
  end

  describe "sync/2" do
    test "fails once after the fault is injected" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      faulty = Disk.inject(disk, :fail_next_sync)

      synced = Disk.sync(faulty, handle)

      assert {:error, %Disk{}, :eio} = synced
    end

    test "succeeds again after the injected failure" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      {:error, failed, :eio} = disk |> Disk.inject(:fail_next_sync) |> Disk.sync(handle)

      synced = Disk.sync(failed, handle)

      assert {:ok, %Disk{}} = synced
    end
  end

  describe "write_at/4" do
    test "fails once after the fault is injected" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      faulty = Disk.inject(disk, :fail_next_write)

      written = Disk.write_at(faulty, handle, 0, "x")

      assert {:error, %Disk{}, :eio} = written
    end

    test "pads with zeros past the end" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      {:ok, written} = Disk.write_at(disk, handle, 3, "ab")

      read = Disk.read_file(written, "/data/file")

      assert {:ok, %Disk{}, <<0, 0, 0, "ab">>} = read
    end

    test "overwrites bytes in the middle" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      {:ok, written} = Disk.write_at(disk, handle, 0, "aaaa")
      {:ok, patched} = Disk.write_at(written, handle, 1, "bb")

      read = Disk.read_file(patched, "/data/file")

      assert {:ok, %Disk{}, "abba"} = read
    end
  end

  describe "reserve/4" do
    test "extends the file with zeros" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      {:ok, written} = Disk.write_at(disk, handle, 0, "ab")
      {:ok, reserved} = Disk.reserve(written, handle, 0, 6)

      read = Disk.read_file(reserved, "/data/file")

      assert {:ok, %Disk{}, <<"ab", 0, 0, 0, 0>>} = read
    end
  end

  describe "list/2" do
    test "fails for a missing directory" do
      disk = Disk.new()

      listed = Disk.list(disk, "/missing")

      assert {:error, %Disk{}, :enoent} = listed
    end

    test "returns the names in a directory" do
      {disk, _handle} = TestUtils.disk_with_file!(:strict, 1)

      listed = Disk.list(disk, "/data")

      assert {:ok, %Disk{}, ["file"]} = listed
    end
  end

  describe "delete/2" do
    test "fails for a missing file" do
      {:ok, disk} = Disk.create_dir(Disk.new(), "/data")

      deleted = Disk.delete(disk, "/data/file")

      assert {:error, %Disk{}, :enoent} = deleted
    end

    test "removes the name" do
      {disk, _handle} = TestUtils.disk_with_file!(:strict, 1)
      {:ok, deleted} = Disk.delete(disk, "/data/file")

      listed = Disk.list(deleted, "/data")

      assert {:ok, %Disk{}, []} = listed
    end
  end

  describe "sync_dir/2" do
    test "fails for a missing directory" do
      disk = Disk.new()

      synced = Disk.sync_dir(disk, "/missing")

      assert {:error, %Disk{}, :enoent} = synced
    end
  end

  describe "crash/1" do
    test "strict loses every write after the last sync" do
      disk = TestUtils.disk_with_pending_write!(:strict, 1)

      bytes = TestUtils.file_after_crash(disk)

      assert bytes == "aaaa"
    end

    test "strict loses a file whose directory was not synced" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      {:ok, written} = Disk.write_at(disk, handle, 0, "aaaa")
      {:ok, synced} = Disk.sync(written, handle)

      bytes = TestUtils.file_after_crash(synced)

      assert bytes == nil
    end

    test "strict keeps a file whose directory was synced" do
      {disk, handle} = TestUtils.disk_with_file!(:strict, 1)
      {:ok, written} = Disk.write_at(disk, handle, 0, "aaaa")
      {:ok, synced} = Disk.sync(written, handle)
      {:ok, named} = Disk.sync_dir(synced, "/data")

      bytes = TestUtils.file_after_crash(named)

      assert bytes == "aaaa"
    end

    test "torn leaves a cut piece of the last write behind" do
      seen? = TestUtils.torn_write_seen?(1..60)

      assert seen?
    end

    test "gives the same result for the same seed" do
      older = TestUtils.disk_with_pending_write!(:subset, 9)
      newer = TestUtils.disk_with_pending_write!(:subset, 9)

      older_bytes = TestUtils.file_after_crash(older)
      newer_bytes = TestUtils.file_after_crash(newer)

      assert older_bytes == newer_bytes
    end
  end
end
