defmodule Keel.Env.Disk.PosixTest do
  use ExUnit.Case, async: true

  alias Keel.Env.Disk.Posix

  @moduletag :tmp_dir

  describe "open/3" do
    test "fails to create a file that exists with exclusive", %{tmp_dir: dir} do
      path = Path.join(dir, "seg")
      File.write!(path, "")

      disk = Posix.new()

      opened = Posix.open(disk, path, ~w[write create exclusive]a)

      assert {:error, %Posix{}, :eexist} = opened
    end
  end

  describe "write_at/4" do
    test "writes at the offset given", %{tmp_dir: dir} do
      path = Path.join(dir, "seg")
      disk = Posix.new()
      {:ok, opened, handle} = Posix.open(disk, path, [:write, :create])
      {:ok, written} = Posix.write_at(opened, handle, 0, "aaaa")
      {:ok, patched} = Posix.write_at(written, handle, 1, "bb")

      read = Posix.read_at(patched, handle, 0, 4)

      assert {:ok, %Posix{}, "abba"} = read
    end
  end

  describe "truncate/3" do
    test "keeps the bytes before the length", %{tmp_dir: dir} do
      path = Path.join(dir, "seg")
      File.write!(path, "abcdef")
      {:ok, opened, handle} = Posix.open(Posix.new(), path, [:write])

      {:ok, _cut} = Posix.truncate(opened, handle, 2)
      kept = File.read!(path)

      assert kept == "ab"
    end
  end

  describe "sync_dir/2" do
    test "fails for a missing directory", %{tmp_dir: dir} do
      path = Path.join(dir, "missing")
      disk = Posix.new()

      synced = Posix.sync_dir(disk, path)

      assert {:error, %Posix{}, :enoent} = synced
    end

    test "syncs a directory", %{tmp_dir: dir} do
      disk = Posix.new()

      synced = Posix.sync_dir(disk, dir)

      assert {:ok, %Posix{}} = synced
    end
  end
end
