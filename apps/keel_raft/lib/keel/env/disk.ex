defmodule Keel.Env.Disk do
  @moduledoc "Every call takes and returns the disk value so the simulated disk can be a plain struct"

  @type t :: struct()

  @type handle :: term()

  @type error :: atom()

  @type mode :: :read | :write | :create | :exclusive

  @type result :: {:ok, t()} | {:error, t(), error()}

  @type result(value) :: {:ok, t(), value} | {:error, t(), error()}

  @type read :: result(binary()) | :eof

  @callback open(t(), Path.t(), [mode()]) :: result(handle())

  @callback close(t(), handle()) :: {:ok, t()}

  @callback write_at(t(), handle(), non_neg_integer(), iodata()) :: result()

  @callback read_at(t(), handle(), non_neg_integer(), non_neg_integer()) :: read()

  @callback read_file(t(), Path.t()) :: result(binary())

  @callback sync(t(), handle()) :: result()

  @callback reserve(t(), handle(), non_neg_integer(), non_neg_integer()) :: result()

  @callback truncate(t(), handle(), non_neg_integer()) :: result()

  @callback rename(t(), Path.t(), Path.t()) :: result()

  @callback delete(t(), Path.t()) :: result()

  @callback list(t(), Path.t()) :: result([String.t()])

  @callback create_dir(t(), Path.t()) :: result()

  @callback sync_dir(t(), Path.t()) :: result()

  @callback size(t(), handle()) :: result(non_neg_integer())
end
