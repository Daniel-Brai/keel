defmodule Keel.Log.Codec do
  @moduledoc "Decoders check every length before they slice so bad input never raises"

  alias Keel.Log.Codec.Header
  alias Keel.Log.Codec.Record
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState

  @magic <<"KEELLOG", 0>>
  @format_version 1
  @header_len 64
  @record_header_len 24
  @checksum_kind 1
  @segment_mod 4_294_967_296

  @type error ::
          :short
          | :too_long
          | :bad_crc
          | :bad_flags
          | :bad_reserved
          | :wrong_segment
          | :unknown_type
          | :bad_payload
          | :bad_magic
          | :bad_version
          | :bad_header_len
          | :bad_checksum_kind

  @spec header_len() :: pos_integer()
  def header_len, do: @header_len

  @spec record_header_len() :: pos_integer()
  def record_header_len, do: @record_header_len

  @spec encode_header(Header.t()) :: binary()
  def encode_header(%Header{} = header) do
    head =
      <<@magic::binary, @format_version::16, @header_len::16, @checksum_kind, 0::24,
        header.segment::64, header.cluster_id::binary-size(16), header.member_tag::binary-size(8),
        header.first_seq::64, 0::32>>

    <<head::binary, :erlang.crc32(head)::32>>
  end

  @spec decode_header(binary()) :: {:ok, Header.t()} | {:error, error()}
  def decode_header(<<head::binary-size(60), crc::32>>) do
    <<magic::binary-size(8), version::16, len::16, kind, reserved::24, segment::64,
      cluster_id::binary-size(16), member_tag::binary-size(8), first_seq::64, tail::32>> = head

    cond do
      magic != @magic ->
        {:error, :bad_magic}

      version != @format_version ->
        {:error, :bad_version}

      len != @header_len ->
        {:error, :bad_header_len}

      kind != @checksum_kind ->
        {:error, :bad_checksum_kind}

      reserved != 0 or tail != 0 ->
        {:error, :bad_reserved}

      :erlang.crc32(head) != crc ->
        {:error, :bad_crc}

      true ->
        {:ok,
         %Header{
           segment: segment,
           cluster_id: cluster_id,
           member_tag: member_tag,
           first_seq: first_seq
         }}
    end
  end

  def decode_header(_other), do: {:error, :short}

  @spec encode_record(Record.body(), pos_integer(), pos_integer()) :: binary()
  def encode_record(body, seq, segment) do
    {type, payload} = encode_body(body)
    head = <<byte_size(payload)::32, type, 0, 0::16, rem(segment, @segment_mod)::32, seq::64>>
    crc = checksum(head, payload)

    <<crc::32, head::binary, payload::binary>>
  end

  @spec decode_record(binary(), pos_integer(), pos_integer()) ::
          {:ok, Record.t(), binary()} | :end | {:error, error()}
  def decode_record(<<0::size(192), _rest::binary>>, _segment, _max_bytes), do: :end

  def decode_record(binary, segment, max_bytes) do
    with {:ok, head, payload, tail} <- split_record(binary, max_bytes),
         :ok <- check_crc(head.crc, head.bytes, payload),
         :ok <- check_flags(head.flags, head.reserved),
         :ok <- check_segment(head.segment, segment),
         {:ok, body} <- decode_body(head.type, payload) do
      record = %Record{body: body, seq: head.seq, size: @record_header_len + head.len}

      {:ok, record, tail}
    end
  end

  defp split_record(<<_crc::32, len::32, _rest::binary>>, max_bytes) when len > max_bytes,
    do: {:error, :too_long}

  defp split_record(<<crc::32, bytes::binary-size(20), rest::binary>>, _max_bytes) do
    <<len::32, type, flags, reserved::16, segment::32, seq::64>> = bytes

    head = %{
      crc: crc,
      bytes: bytes,
      len: len,
      type: type,
      flags: flags,
      reserved: reserved,
      segment: segment,
      seq: seq
    }

    case rest do
      <<payload::binary-size(^len), tail::binary>> -> {:ok, head, payload, tail}
      _short -> {:error, :short}
    end
  end

  defp split_record(_short, _max_bytes), do: {:error, :short}

  defp encode_body({:entries, [_ | _] = entries}) do
    encoded = Enum.map(entries, &encode_entry/1)
    {1, <<length(entries)::32, :erlang.list_to_binary(encoded)::binary>>}
  end

  defp encode_body({:hard_state, %HardState{} = state}) do
    vote = vote_bytes(state.voted_for)
    {2, <<state.term::64, byte_size(vote)::16, vote::binary, state.commit::64>>}
  end

  defp encode_body({:truncate, from}), do: {3, <<from::64>>}

  defp encode_body({:compacted, keep_from}), do: {5, <<keep_from::64>>}

  defp encode_body({:sync_mark, segment, offset, seq}),
    do: {4, <<segment::64, offset::64, seq::64>>}

  defp vote_bytes(nil), do: ""
  defp vote_bytes(id), do: id

  defp encode_entry(%Entry{} = entry) do
    <<entry.index::64, entry.term::64, kind_byte(entry.kind), entry.time_ms::64,
      byte_size(entry.data)::32, entry.data::binary>>
  end

  defp kind_byte(:command), do: 0
  defp kind_byte(:noop), do: 1
  defp kind_byte(:config), do: 2

  defp check_crc(crc, head, payload) do
    if checksum(head, payload) == crc, do: :ok, else: {:error, :bad_crc}
  end

  defp checksum(head, payload), do: head |> :erlang.crc32() |> :erlang.crc32(payload)

  defp check_flags(0, 0), do: :ok
  defp check_flags(0, _reserved), do: {:error, :bad_reserved}
  defp check_flags(_flags, _reserved), do: {:error, :bad_flags}

  defp check_segment(seg, segment) do
    if seg == rem(segment, @segment_mod), do: :ok, else: {:error, :wrong_segment}
  end

  defp decode_body(1, <<count::32, rest::binary>>) when count > 0 do
    case decode_entries(rest, count, []) do
      {:ok, entries} -> {:ok, {:entries, entries}}
      :error -> {:error, :bad_payload}
    end
  end

  defp decode_body(2, <<term::64, len::16, vote::binary-size(len), commit::64>>) do
    if String.valid?(vote) do
      voted_for = if len == 0, do: nil, else: vote
      {:ok, {:hard_state, %HardState{term: term, voted_for: voted_for, commit: commit}}}
    else
      {:error, :bad_payload}
    end
  end

  defp decode_body(3, <<from::64>>), do: {:ok, {:truncate, from}}

  defp decode_body(5, <<keep_from::64>>), do: {:ok, {:compacted, keep_from}}

  defp decode_body(4, <<segment::64, offset::64, seq::64>>),
    do: {:ok, {:sync_mark, segment, offset, seq}}

  defp decode_body(type, _payload) when type in 1..5, do: {:error, :bad_payload}
  defp decode_body(_type, _payload), do: {:error, :unknown_type}

  defp decode_entries(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_entries(
         <<index::64, term::64, kind, time_ms::64, len::32, data::binary-size(len),
           rest::binary>>,
         count,
         acc
       )
       when count > 0 and kind in 0..2 do
    entry = %Entry{
      index: index,
      term: term,
      kind: kind_atom(kind),
      data: data,
      time_ms: time_ms
    }

    decode_entries(rest, count - 1, [entry | acc])
  end

  defp decode_entries(_rest, _count, _acc), do: :error

  defp kind_atom(0), do: :command
  defp kind_atom(1), do: :noop
  defp kind_atom(2), do: :config
end
