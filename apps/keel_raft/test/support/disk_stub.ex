defmodule Keel.Env.Disk.Stub do
  @behaviour Keel.Env.Disk

  alias Keel.Env.Disk.Posix

  @fields [:inner, :counters]
  @enforce_keys @fields
  defstruct @fields

  def new, do: %__MODULE__{inner: Posix.new(), counters: :counters.new(3, [])}

  # A disk that never syncs, so a power loss test has something it must catch
  def new(:no_sync) do
    disk = new()

    :counters.put(disk.counters, 3, 1)

    disk
  end

  def syncs(%__MODULE__{counters: counters}), do: :counters.get(counters, 1)

  def fail_syncs(%__MODULE__{counters: counters}), do: :counters.put(counters, 2, 1)

  @impl Keel.Env.Disk
  def open(disk, path, modes) do
    reply = Posix.open(disk.inner, path, modes)

    with_value(disk, reply)
  end

  @impl Keel.Env.Disk
  def close(disk, handle) do
    reply = Posix.close(disk.inner, handle)

    wrap(disk, reply)
  end

  @impl Keel.Env.Disk
  def write_at(disk, handle, offset, data) do
    reply = Posix.write_at(disk.inner, handle, offset, data)

    wrap(disk, reply)
  end

  @impl Keel.Env.Disk
  def read_at(disk, handle, offset, length) do
    reply = Posix.read_at(disk.inner, handle, offset, length)

    with_value(disk, reply)
  end

  @impl Keel.Env.Disk
  def read_file(disk, path) do
    reply = Posix.read_file(disk.inner, path)

    with_value(disk, reply)
  end

  @impl Keel.Env.Disk
  def sync(disk, handle) do
    :counters.add(disk.counters, 1, 1)
    failing = :counters.get(disk.counters, 2)
    skipping = :counters.get(disk.counters, 3)

    case {failing, skipping} do
      {1, _skipping} ->
        {:error, disk, :eio}

      {_failing, 1} ->
        {:ok, disk}

      {_failing, _skipping} ->
        reply = Posix.sync(disk.inner, handle)

        wrap(disk, reply)
    end
  end

  @impl Keel.Env.Disk
  def reserve(disk, handle, offset, length) do
    reply = Posix.reserve(disk.inner, handle, offset, length)

    wrap(disk, reply)
  end

  @impl Keel.Env.Disk
  def truncate(disk, handle, length) do
    reply = Posix.truncate(disk.inner, handle, length)

    wrap(disk, reply)
  end

  @impl Keel.Env.Disk
  def rename(disk, from, to) do
    reply = Posix.rename(disk.inner, from, to)

    wrap(disk, reply)
  end

  @impl Keel.Env.Disk
  def delete(disk, path) do
    reply = Posix.delete(disk.inner, path)

    wrap(disk, reply)
  end

  @impl Keel.Env.Disk
  def list(disk, path) do
    reply = Posix.list(disk.inner, path)

    with_value(disk, reply)
  end

  @impl Keel.Env.Disk
  def create_dir(disk, path) do
    reply = Posix.create_dir(disk.inner, path)

    wrap(disk, reply)
  end

  @impl Keel.Env.Disk
  def sync_dir(disk, path) do
    skipping = :counters.get(disk.counters, 3)

    case skipping do
      1 ->
        {:ok, disk}

      _syncing ->
        reply = Posix.sync_dir(disk.inner, path)

        wrap(disk, reply)
    end
  end

  @impl Keel.Env.Disk
  def size(disk, handle) do
    reply = Posix.size(disk.inner, handle)

    with_value(disk, reply)
  end

  defp wrap(disk, {:ok, inner}), do: {:ok, %{disk | inner: inner}}
  defp wrap(disk, {:error, inner, reason}), do: {:error, %{disk | inner: inner}, reason}

  defp with_value(disk, {:ok, inner, value}), do: {:ok, %{disk | inner: inner}, value}
  defp with_value(disk, {:error, inner, reason}), do: {:error, %{disk | inner: inner}, reason}
  defp with_value(_disk, :eof), do: :eof
end
