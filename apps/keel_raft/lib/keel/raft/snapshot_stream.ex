defmodule Keel.Raft.SnapshotStream do
  @moduledoc "Which chunks of a snapshot file may be sent next"

  alias Keel.Common.T
  alias Keel.Raft.Message.InstallSnapshot
  alias Keel.Raft.SnapshotMeta

  @fields ~w[meta term seq from to chunk_bytes max_inflight next acked]a
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          meta: SnapshotMeta.t(),
          term: T.term_number(),
          seq: pos_integer(),
          from: T.member_id(),
          to: T.member_id(),
          chunk_bytes: pos_integer(),
          max_inflight: pos_integer(),
          next: non_neg_integer(),
          acked: non_neg_integer()
        }

  @type range :: {non_neg_integer(), non_neg_integer()}

  @type option ::
          {:meta, SnapshotMeta.t()}
          | {:term, T.term_number()}
          | {:seq, pos_integer()}
          | {:from, T.member_id()}
          | {:to, T.member_id()}
          | {:chunk_bytes, pos_integer()}
          | {:max_inflight, pos_integer()}

  @spec new([option()]) :: t()
  def new(opts) do
    %__MODULE__{
      meta: Keyword.fetch!(opts, :meta),
      term: Keyword.fetch!(opts, :term),
      seq: Keyword.fetch!(opts, :seq),
      from: Keyword.fetch!(opts, :from),
      to: Keyword.fetch!(opts, :to),
      chunk_bytes: Keyword.fetch!(opts, :chunk_bytes),
      max_inflight: Keyword.fetch!(opts, :max_inflight),
      next: 0,
      acked: 0
    }
  end

  @doc "The ranges to send now. An empty file still has one range of no bytes."
  @spec pull(t()) :: {t(), [range()]}
  def pull(%__MODULE__{meta: %{size: 0}, next: 0} = stream), do: {%{stream | next: 1}, [{0, 0}]}

  def pull(%__MODULE__{} = stream) do
    room = stream.max_inflight - inflight(stream)
    ranges = ranges(stream, stream.next, room, [])
    sent = Enum.sum_by(ranges, fn {_offset, length} -> length end)

    {%{stream | next: stream.next + sent}, ranges}
  end

  @doc "An offset above the acknowledged one is progress. A lower one means the follower lost bytes."
  @spec ack(t(), non_neg_integer()) :: t()
  def ack(%__MODULE__{acked: acked} = stream, offset) when offset > acked do
    next = max(stream.next, offset)

    %{stream | acked: offset, next: next}
  end

  def ack(%__MODULE__{acked: acked} = stream, offset) when offset < acked,
    do: %{stream | acked: offset, next: offset}

  def ack(%__MODULE__{} = stream, _offset), do: stream

  @spec restart(t()) :: t()
  def restart(%__MODULE__{} = stream), do: %{stream | next: 0, acked: 0}

  @spec complete?(t()) :: boolean()
  def complete?(%__MODULE__{meta: meta, acked: acked}), do: acked >= meta.size and meta.size > 0

  @spec message(t(), non_neg_integer(), binary()) :: InstallSnapshot.t()
  def message(%__MODULE__{} = stream, offset, data) do
    %InstallSnapshot{
      term: stream.term,
      from: stream.from,
      to: stream.to,
      meta: stream.meta,
      offset: offset,
      data: data,
      done?: offset + byte_size(data) >= stream.meta.size,
      seq: stream.seq
    }
  end

  defp inflight(%__MODULE__{next: next, acked: acked, chunk_bytes: chunk}),
    do: div(next - acked + chunk - 1, chunk)

  defp ranges(_stream, _offset, room, acc) when room <= 0, do: Enum.reverse(acc)

  defp ranges(%__MODULE__{meta: %{size: size}} = stream, offset, room, acc) when offset < size do
    length = min(stream.chunk_bytes, size - offset)

    ranges(stream, offset + length, room - 1, [{offset, length} | acc])
  end

  defp ranges(_stream, _offset, _room, acc), do: Enum.reverse(acc)
end
