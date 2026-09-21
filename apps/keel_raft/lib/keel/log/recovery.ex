defmodule Keel.Log.Recovery do
  @moduledoc "A bad record in the newest segment is a torn tail unless a later sync mark proves it was durable"

  alias Keel.Common.T
  alias Keel.Log.Codec
  alias Keel.Log.Codec.Header
  alias Keel.Log.Codec.Record
  alias Keel.Log.Recovery.Result
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.SnapshotMeta

  @mark_pattern <<0, 0, 0, 24, 4, 0, 0, 0>>

  @type segment :: {pos_integer(), binary()}

  @type expect :: %{
          cluster_id: <<_::128>>,
          member_tag: <<_::64>>,
          max_record_bytes: pos_integer()
        }

  @type error :: {:log_corrupt, non_neg_integer(), non_neg_integer(), term()}

  @type state :: %{
          seq: pos_integer(),
          hard_state: HardState.t(),
          entries: %{T.index() => Entry.t()},
          last: T.index(),
          proven: non_neg_integer(),
          recent: [Result.tail_record()],
          torn?: boolean(),
          last_segment: nil | pos_integer(),
          anchor: nil | {pos_integer(), non_neg_integer()},
          floor: T.index(),
          keep_from: T.index(),
          maxes: %{pos_integer() => T.index()}
        }

  @doc "The snapshot is the newest whole file on disk or nil."
  @spec recover([segment()], expect(), nil | SnapshotMeta.t()) ::
          {:ok, Result.t()} | {:error, error()}
  def recover(segments, expect, snapshot \\ nil) do
    sorted = Enum.sort_by(segments, &elem(&1, 0))

    with :ok <- check_contiguous(sorted),
         {:ok, headed, dropped} <- read_headers(sorted, expect),
         {:ok, state} <- scan_segments(headed, expect),
         {:ok, kept} <- reconcile(state, snapshot) do
      {:ok, build_result(state, headed, dropped, kept)}
    end
  end

  defp check_contiguous([{first, _a}, {second, _b} | _rest] = segments)
       when second == first + 1 do
    rest = tl(segments)

    check_contiguous(rest)
  end

  defp check_contiguous([{first, _a}, {second, _b} | _rest]) when second != first + 1,
    do: {:error, {:log_corrupt, second, 0, :segment_gap}}

  defp check_contiguous(_segments), do: :ok

  defp read_headers([], _expect), do: {:ok, [], []}

  defp read_headers(sorted, expect) do
    {older, [{number, binary}]} = Enum.split(sorted, -1)

    with {:ok, headed} <- read_older(older, expect, []),
         {:ok, newest, dropped} <- read_newest(number, binary, expect) do
      {:ok, headed ++ newest, dropped}
    end
  end

  defp read_older([], _expect, acc), do: {:ok, Enum.reverse(acc)}

  defp read_older([{number, binary} | rest], expect, acc) do
    case read_header(number, binary, expect) do
      {:ok, headed} -> read_older(rest, expect, [headed | acc])
      {:unreadable, reason, _body} -> {:error, {:log_corrupt, number, 0, reason}}
      {:error, _corrupt} = error -> error
    end
  end

  defp read_newest(number, binary, expect) do
    case read_header(number, binary, expect) do
      {:ok, headed} -> {:ok, [headed], []}
      {:unreadable, reason, body} -> drop_or_fail(number, reason, body, expect)
      {:error, _corrupt} = error -> error
    end
  end

  defp drop_or_fail(number, reason, body, expect) do
    case Codec.decode_record(body, number, expect.max_record_bytes) do
      {:ok, _record, _rest} -> {:error, {:log_corrupt, number, 0, reason}}
      _other -> {:ok, [], [number]}
    end
  end

  defp read_header(number, <<head::binary-size(64), body::binary>>, expect) do
    case Codec.decode_header(head) do
      {:ok, header} -> check_header(number, header, body, expect)
      {:error, reason} -> {:unreadable, reason, body}
    end
  end

  defp read_header(_number, _short, _expect), do: {:unreadable, :short, <<>>}

  defp check_header(number, %Header{segment: number} = header, body, expect) do
    if header.cluster_id == expect.cluster_id and header.member_tag == expect.member_tag,
      do: {:ok, {number, header, body}},
      else: {:error, {:log_corrupt, number, 0, :wrong_identity}}
  end

  defp check_header(number, _header, _body, _expect),
    do: {:error, {:log_corrupt, number, 0, :wrong_segment_number}}

  defp scan_segments(headed, expect) do
    active = active_segments(headed)
    first = first_seq(active)

    state = %{
      seq: first,
      hard_state: %HardState{term: 0, voted_for: nil, commit: 0},
      entries: %{},
      last: 0,
      proven: first - 1,
      recent: [],
      torn?: false,
      last_segment: nil,
      anchor: nil,
      floor: 1,
      keep_from: 1,
      maxes: %{}
    }

    scan_each(active, state, expect)
  end

  # A newer segment replaces every record of an older one from its first sequence number on
  defp active_segments(headed) do
    {limited, _limit} =
      headed
      |> Enum.reverse()
      |> Enum.map_reduce(nil, fn {_number, header, _body} = segment, limit ->
        {{segment, limit}, min_limit(limit, header.first_seq)}
      end)

    limited
    |> Enum.reverse()
    |> Enum.reject(fn {{_number, header, _body}, limit} ->
      limit != nil and header.first_seq >= limit
    end)
  end

  defp min_limit(nil, seq), do: seq
  defp min_limit(limit, seq), do: min(limit, seq)

  defp first_seq([{{_number, header, _body}, _limit} | _rest]), do: header.first_seq
  defp first_seq([]), do: 1

  defp scan_each([], state, _expect), do: {:ok, state}

  defp scan_each([{{number, header, body}, limit} | rest], state, expect) do
    with :ok <- check_start(number, header, state),
         {:ok, scanned} <- scan_records(body, 64, number, limit, state, expect) do
      scan_each(rest, %{scanned | last_segment: number}, expect)
    end
  end

  defp check_start(_number, %Header{first_seq: seq}, %{seq: seq}), do: :ok
  defp check_start(number, _header, _state), do: {:error, {:log_corrupt, number, 0, :seq_gap}}

  defp scan_records(_data, _offset, _number, limit, %{seq: limit} = state, _expect),
    do: {:ok, state}

  defp scan_records(data, offset, number, limit, state, expect) do
    case Codec.decode_record(data, number, expect.max_record_bytes) do
      {:ok, %Record{seq: seq} = record, rest} when seq == state.seq ->
        continue(record, rest, offset, number, limit, state, expect)

      {:ok, _record, _rest} ->
        stop(:seq_gap, data, offset, number, limit, state, expect)

      :end ->
        stop(:end, data, offset, number, limit, state, expect)

      {:error, :short} ->
        short = if zeros?(data), do: :end, else: :short

        stop(short, data, offset, number, limit, state, expect)

      {:error, reason} ->
        stop(reason, data, offset, number, limit, state, expect)
    end
  end

  defp continue(record, rest, offset, number, limit, state, expect) do
    case apply_record(record, state, number) do
      {:ok, applied} -> scan_records(rest, offset + record.size, number, limit, applied, expect)
      {:error, reason} -> {:error, {:log_corrupt, number, offset, reason}}
    end
  end

  defp stop(reason, _data, offset, number, limit, _state, _expect) when limit != nil,
    do: {:error, {:log_corrupt, number, offset, reason}}

  defp stop(reason, data, offset, number, nil, state, expect) do
    if durable_mark_after?(data, number, state.seq, expect),
      do: {:error, {:log_corrupt, number, offset, reason}},
      else: {:ok, %{state | torn?: reason != :end}}
  end

  defp zeros?(data), do: data == <<0::size(byte_size(data) * 8)>>

  defp durable_mark_after?(data, number, seq, expect) do
    data
    |> :binary.matches(@mark_pattern)
    |> Enum.any?(fn {position, _length} -> mark_at?(data, position - 4, number, seq, expect) end)
  end

  defp mark_at?(_data, start, _number, _seq, _expect) when start < 0, do: false

  defp mark_at?(data, start, number, seq, expect) do
    candidate = binary_part(data, start, byte_size(data) - start)

    case Codec.decode_record(candidate, number, expect.max_record_bytes) do
      {:ok, %Record{body: {:sync_mark, _segment, _offset, marked}}, _rest} -> marked >= seq
      _other -> false
    end
  end

  defp apply_record(%Record{body: {:entries, entries}} = record, state, number) do
    with {:ok, appended} <- append(entries, state, number),
         do: {:ok, advance(note_max(appended, number), record)}
  end

  defp apply_record(%Record{body: {:hard_state, hard_state}} = record, state, _number),
    do: {:ok, advance(%{state | hard_state: hard_state}, record)}

  defp apply_record(%Record{body: {:truncate, from}}, _state, _number) when from < 1,
    do: {:error, :bad_truncate}

  defp apply_record(%Record{body: {:truncate, from}} = record, state, _number) do
    truncated = truncate(state, from)

    {:ok, advance(truncated, record)}
  end

  defp apply_record(%Record{body: {:compacted, keep_from}}, _state, _number) when keep_from < 1,
    do: {:error, :bad_compaction}

  defp apply_record(%Record{body: {:compacted, keep_from}} = record, state, _number) do
    compacted = compact(state, keep_from)

    {:ok, advance(compacted, record)}
  end

  defp apply_record(
         %Record{body: {:sync_mark, _segment, _offset, marked}} = record,
         state,
         _number
       ) do
    proven = max(state.proven, min(marked, record.seq - 1))

    {:ok, advance(%{state | proven: proven}, record)}
  end

  defp advance(state, %Record{seq: seq, body: body}) do
    recent =
      Enum.take_while([{seq, body} | state.recent], fn {at, _body} -> at > state.proven end)

    %{state | seq: seq + 1, recent: recent}
  end

  # Entries that follow an empty log may start anywhere. The end of the scan checks them
  defp append([first | _rest] = entries, %{entries: held} = state, number) when held == %{} do
    anchored = %{state | anchor: {number, 0}, last: first.index - 1, floor: first.index}

    append_held(entries, anchored)
  end

  defp append(entries, state, _number), do: append_held(entries, state)

  defp append_held(entries, state) do
    if continues?(entries, state.last) do
      added = Map.new(entries, &{&1.index, &1})
      {:ok, %{state | entries: Map.merge(state.entries, added), last: List.last(entries).index}}
    else
      {:error, :index_gap}
    end
  end

  defp continues?(entries, last) do
    indexes = Enum.map(entries, & &1.index)
    count = length(entries)
    expected = Enum.to_list((last + 1)..(last + count))

    indexes == expected
  end

  defp truncate(%{last: last} = state, from) when from > last, do: state

  defp truncate(state, from) do
    kept = Enum.reduce(from..state.last, state.entries, &Map.delete(&2, &1))

    %{state | entries: kept, last: from - 1}
  end

  defp note_max(state, number) do
    maxes = Map.update(state.maxes, number, state.last, &max(&1, state.last))

    %{state | maxes: maxes}
  end

  defp compact(state, keep_from) do
    kept = Map.reject(state.entries, fn {at, _entry} -> at < keep_from end)

    %{
      state
      | entries: kept,
        last: max(state.last, keep_from - 1),
        keep_from: max(state.keep_from, keep_from)
    }
  end

  # The entries from the last compaction point stay when the log holds the snapshot's last entry
  # or was compacted against that snapshot. Any other log is dropped.
  defp reconcile(%{floor: floor, keep_from: keep, anchor: {number, _offset}}, _snapshot)
       when floor > keep,
       do: {:error, {:log_corrupt, number, 0, :missing_prefix}}

  defp reconcile(%{keep_from: 1} = state, nil), do: {:ok, {state.entries, 1, state.last, nil}}

  defp reconcile(state, nil),
    do: {:error, {:log_corrupt, state.last_segment, 0, :compacted_without_snapshot}}

  defp reconcile(%{keep_from: keep} = state, %SnapshotMeta{index: index} = snapshot)
       when keep - 1 > index,
       do: {:error, {:log_corrupt, state.last_segment, 0, {:compacted_past_snapshot, snapshot}}}

  defp reconcile(state, %SnapshotMeta{} = snapshot) do
    held = Map.get(state.entries, snapshot.index)
    matches? = held != nil and held.term == snapshot.term

    if matches? or state.keep_from == snapshot.index + 1,
      do: {:ok, {state.entries, state.keep_from, state.last, snapshot}},
      else: {:ok, {%{}, snapshot.index + 1, snapshot.index, snapshot}}
  end

  defp build_result(state, headed, dropped, {entries, first, last, snapshot}) do
    list = Enum.map(first..last//1, &Map.fetch!(entries, &1))
    hard_state = %{state.hard_state | commit: min(state.hard_state.commit, last)}

    segments =
      for {number, _header, _body} <- headed, do: {number, Map.get(state.maxes, number, 0)}

    %Result{
      hard_state: hard_state,
      entries: list,
      last_segment: state.last_segment,
      next_seq: state.seq,
      tail: Enum.reverse(state.recent),
      dropped: dropped,
      torn_tail?: state.torn?,
      first_index: first,
      snapshot: snapshot,
      keep_from: state.keep_from,
      segments: segments
    }
  end
end
