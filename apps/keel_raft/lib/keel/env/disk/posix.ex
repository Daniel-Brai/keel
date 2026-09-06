defmodule Keel.Env.Disk.Posix do
  @behaviour Keel.Env.Disk

  defstruct []

  @type t :: %__MODULE__{}

  # Raw handles skip the file server. A raw handle works only in the process that opened it
  @open_flags [:raw, :binary]

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @impl Keel.Env.Disk
  def open(disk, path, modes) do
    flags = Enum.flat_map(modes, &flag/1)

    case :file.open(path, @open_flags ++ flags) do
      {:ok, handle} -> {:ok, disk, handle}
      {:error, reason} -> {:error, disk, reason}
    end
  end

  @impl Keel.Env.Disk
  def close(disk, handle) do
    :ok = :file.close(handle)
    {:ok, disk}
  end

  @impl Keel.Env.Disk
  def write_at(disk, handle, offset, data) do
    reply = :file.pwrite(handle, offset, data)

    result(disk, reply)
  end

  @impl Keel.Env.Disk
  def read_at(disk, handle, offset, length) do
    case :file.pread(handle, offset, length) do
      {:ok, data} -> {:ok, disk, data}
      :eof -> :eof
      {:error, reason} -> {:error, disk, reason}
    end
  end

  @impl Keel.Env.Disk
  def read_file(disk, path) do
    case File.read(path) do
      {:ok, data} -> {:ok, disk, data}
      {:error, reason} -> {:error, disk, reason}
    end
  end

  @impl Keel.Env.Disk
  def sync(disk, handle) do
    reply = :file.datasync(handle)

    result(disk, reply)
  end

  @impl Keel.Env.Disk
  def reserve(disk, handle, offset, length) do
    reply = :file.allocate(handle, offset, length)

    result(disk, reply)
  end

  @impl Keel.Env.Disk
  def truncate(disk, handle, length) do
    reply = cut(handle, length)

    result(disk, reply)
  end

  @impl Keel.Env.Disk
  def rename(disk, from, to) do
    reply = :file.rename(from, to)

    result(disk, reply)
  end

  @impl Keel.Env.Disk
  def delete(disk, path) do
    reply = :file.delete(path)

    result(disk, reply)
  end

  @impl Keel.Env.Disk
  def list(disk, path) do
    case File.ls(path) do
      {:ok, names} -> {:ok, disk, Enum.sort(names)}
      {:error, reason} -> {:error, disk, reason}
    end
  end

  @impl Keel.Env.Disk
  def create_dir(disk, path) do
    reply = File.mkdir_p(path)

    result(disk, reply)
  end

  # Syncing the directory makes a new or renamed file survive a crash
  @impl Keel.Env.Disk
  def sync_dir(disk, path) do
    with {:ok, handle} <- :file.open(path, ~w[read raw directory]a),
         sync <- :file.sync(handle),
         :ok <- :file.close(handle) do
      result(disk, sync)
    else
      {:error, reason} -> {:error, disk, reason}
    end
  end

  @impl Keel.Env.Disk
  def size(disk, handle) do
    case :file.position(handle, :eof) do
      {:ok, size} -> {:ok, disk, size}
      {:error, reason} -> {:error, disk, reason}
    end
  end

  # write_at never uses :append. On Linux O_APPEND makes write_at ignore its offset
  defp flag(:read), do: [:read]
  defp flag(:write), do: [:read, :write]
  defp flag(:create), do: [:write]
  defp flag(:exclusive), do: [:exclusive]

  defp cut(handle, length) do
    case :file.position(handle, length) do
      {:ok, _at} -> :file.truncate(handle)
      {:error, _reason} = failed -> failed
    end
  end

  defp result(disk, :ok), do: {:ok, disk}
  defp result(disk, {:error, reason}), do: {:error, disk, reason}
end
