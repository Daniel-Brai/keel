defmodule Keel.Log.SnapshotTest do
  use ExUnit.Case, async: true

  alias Keel.Env.Disk.Posix
  alias Keel.Log.Snapshot
  alias Keel.TestUtils

  @moduletag :tmp_dir

  describe "latest/2" do
    test "returns nil for a missing directory", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")

      found = Snapshot.latest(Posix.new(), snap)

      assert {:ok, %Posix{}, nil} = found
    end

    test "skips a newer file that is not whole", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")
      older = TestUtils.write_snapshot!(snap, 5, 1)
      TestUtils.garbage_snapshot!(snap, 9, 1)

      found = Snapshot.latest(Posix.new(), snap)

      assert {:ok, %Posix{}, ^older} = found
    end

    test "returns the newest whole file", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")
      TestUtils.write_snapshot!(snap, 5, 1)
      TestUtils.write_snapshot!(snap, 9, 2)

      {:ok, _disk, meta} = Snapshot.latest(Posix.new(), snap)

      assert {meta.index, meta.term} == {9, 2}
    end
  end

  describe "write/6" do
    test "removes older snapshots once the new one is in place", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")
      TestUtils.write_snapshot!(snap, 5, 1)
      TestUtils.write_snapshot!(snap, 9, 1)

      files = TestUtils.snapshot_files(snap)

      assert files == ~w[00000000000000000009-00000000000000000001.snap tmp]
    end

    test "keeps the new file when a newer one is already on disk", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")
      TestUtils.write_snapshot!(snap, 16, 1)
      TestUtils.write_snapshot!(snap, 14, 2)

      files = TestUtils.snapshot_files(snap)

      assert files == ~w[
               00000000000000000014-00000000000000000002.snap
               00000000000000000016-00000000000000000001.snap
               tmp
             ]
    end

    test "keeps the config after the file is found again", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")
      config = TestUtils.random_config(learners: ["m4"])
      written = TestUtils.write_snapshot!(snap, 5, 1, "state", config)

      {:ok, _disk, found} = Snapshot.latest(Posix.new(), snap)

      assert found == written
    end

    test "reads back the payload it wrote", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")
      meta = TestUtils.write_snapshot!(snap, 5, 1, "machine state")

      read = Snapshot.read(Posix.new(), snap, meta)

      assert {:ok, %Posix{}, "machine state"} = read
    end
  end

  describe "meta/1" do
    test "fails for a file with a flipped bit in its config" do
      config = TestUtils.random_config(learners: ["m4"])
      bytes = Snapshot.encode(5, 1, 50, "state", config)
      flipped = TestUtils.flip_bit(bytes, 50 * 8)

      found = Snapshot.meta(flipped)

      assert found == {:error, :bad_crc}
    end

    test "has no config for a file written without one" do
      bytes = Snapshot.encode(5, 1, 50, "state")

      {:ok, meta} = Snapshot.meta(bytes)

      assert meta.config == nil
    end

    test "carries the config the file holds" do
      config = TestUtils.random_config(learners: ["m4"], removed: ["m9"])
      bytes = Snapshot.encode(5, 1, 50, "state", config)

      {:ok, meta} = Snapshot.meta(bytes)

      assert meta.config == config
    end
  end

  describe "verify/2" do
    test "fails for bytes that do not match the meta" do
      bytes = Snapshot.encode(5, 1, 50, "state")
      {:ok, meta} = Snapshot.meta(bytes)
      other = %{meta | checksum: meta.checksum + 1}

      verified = Snapshot.verify(bytes, other)

      assert verified == {:error, :mismatch}
    end

    test "fails for a file with a flipped bit" do
      bytes = Snapshot.encode(5, 1, 50, "state")
      {:ok, meta} = Snapshot.meta(bytes)
      flipped = TestUtils.flip_bit(bytes, bit_size(bytes) - 3)

      verified = Snapshot.verify(flipped, meta)

      assert verified == {:error, :bad_crc}
    end
  end

  describe "clean_tmp/2" do
    test "removes the part files", %{tmp_dir: dir} do
      snap = Path.join(dir, "snap")
      tmp = Path.join(snap, "tmp")
      File.mkdir_p!(tmp)
      part = Path.join(tmp, "x.part")
      File.write!(part, "half")

      {:ok, _disk} = Snapshot.clean_tmp(Posix.new(), snap)

      assert TestUtils.snapshot_files(tmp) == []
    end
  end
end
