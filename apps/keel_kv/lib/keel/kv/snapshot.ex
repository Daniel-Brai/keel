defmodule Keel.KV.Snapshot do
  @moduledoc "Records come in key order and sessions in id order so equal state gives equal bytes"

  alias Keel.KV.Limits
  alias Keel.KV.Result
  alias Keel.KV.Session
  alias Keel.KV.State

  @magic "KEELKVSN"
  @format 1
  @trailer 255

  @spec encode(State.t()) :: binary()
  def encode(%State{} = state) do
    pairs = Limits.to_pairs(state.limits)
    records = :gb_trees.to_list(state.tree)
    sessions = :gb_trees.to_list(state.sessions)
    fields = <<state.revision::64, state.data_bytes::64, state.next_sweep::64>>

    sections = [
      {1, length(pairs), Enum.map(pairs, fn {id, value} -> <<id, value::64>> end)},
      {2, length(records), Enum.map(records, &encode_record/1)},
      {3, length(sessions), Enum.map(sessions, &encode_session/1)},
      {4, 1, fields}
    ]

    payloads = Enum.map(sections, fn {_section, _count, payload} -> payload end)
    digest = :crypto.hash(:sha256, payloads)
    totals = <<length(records)::64, length(sessions)::64, digest::binary>>
    trailer = {@trailer, 0, totals}
    frames = sections |> Enum.concat([trailer]) |> Enum.map(&frame/1)

    :erlang.iolist_to_binary([@magic, <<@format::16>> | frames])
  end

  @spec decode(binary()) :: {:ok, State.t()} | {:error, :corrupt_snapshot}
  def decode(bytes) do
    with {:ok, frames_bytes} <- strip_header(bytes),
         {:ok, frames} <- read_frames(frames_bytes, [1, 2, 3, 4, @trailer], []),
         {:ok, state} <- build(frames) do
      {:ok, state}
    else
      _other -> {:error, :corrupt_snapshot}
    end
  end

  defp strip_header(bytes) do
    case bytes do
      <<@magic, @format::16, rest::binary>> -> {:ok, rest}
      _other -> :error
    end
  end

  defp frame({section, count, payload}) do
    body = :erlang.iolist_to_binary(payload)
    head = <<section, count::32, byte_size(body)::32>>
    crc = head |> :erlang.crc32() |> :erlang.crc32(body)

    [head, body, <<crc::32>>]
  end

  defp encode_record({key, {value, revision}}) do
    <<byte_size(key)::16, key::binary, revision::64, byte_size(value)::32, value::binary>>
  end

  defp encode_session({id, %Session{} = session}) do
    results = :gb_trees.to_list(session.results)
    head = <<id::64, session.lowest_seq::64, session.last_seen_ms::64, length(results)::16>>

    [head | Enum.map(results, &encode_result/1)]
  end

  defp encode_result({seq, result}) do
    bytes = Result.encode(result)

    <<seq::64, byte_size(bytes)::32, bytes::binary>>
  end

  defp read_frames(<<>>, [], acc), do: {:ok, Enum.reverse(acc)}
  defp read_frames(_rest, [], _acc), do: :error
  defp read_frames(<<>>, _wanted, _acc), do: :error

  defp read_frames(bytes, [section | wanted], acc) do
    case bytes do
      <<^section, count::32, length::32, payload::binary-size(length), crc::32, rest::binary>> ->
        head = <<section, count::32, length::32>>
        checked = head |> :erlang.crc32() |> :erlang.crc32(payload)

        if checked == crc,
          do: read_frames(rest, wanted, [{section, count, payload} | acc]),
          else: :error

      _other ->
        :error
    end
  end

  defp build([limits, records, sessions, fields, trailer]) do
    with {:ok, limit_map} <- decode_limits(limits),
         {:ok, tree, record_count, data_bytes} <- decode_records(records),
         {:ok, session_tree, session_count} <- decode_sessions(sessions),
         {:ok, revision, held, next_sweep} <- decode_fields(fields),
         :ok <-
           check_trailer(
             [limits, records, sessions, fields],
             trailer,
             record_count,
             session_count
           ),
         true <- held == data_bytes do
      {:ok,
       %State{
         tree: tree,
         sessions: session_tree,
         revision: revision,
         data_bytes: data_bytes,
         next_sweep: next_sweep,
         limits: limit_map
       }}
    else
      _other -> :error
    end
  end

  defp decode_limits({1, count, payload}) do
    with {:ok, pairs} <- limit_pairs(payload, count, []),
         limits = Map.new(pairs),
         true <- map_size(limits) == count,
         true <- map_size(limits) == map_size(Limits.defaults()),
         true <- Limits.valid?(limits) do
      {:ok, limits}
    else
      _other -> :error
    end
  end

  defp limit_pairs(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp limit_pairs(bytes, count, acc) when count > 0 do
    case bytes do
      <<id, value::64, rest::binary>> -> next_limit(Limits.name(id), value, rest, count, acc)
      _other -> :error
    end
  end

  defp limit_pairs(_bytes, _count, _acc), do: :error

  defp next_limit({:ok, name}, value, rest, count, acc),
    do: limit_pairs(rest, count - 1, [{name, value} | acc])

  defp next_limit(:error, _value, _rest, _count, _acc), do: :error

  defp decode_records({2, count, payload}) do
    with {:ok, records} <- read_records(payload, count, nil, []) do
      bytes =
        Enum.reduce(records, 0, fn {key, {value, _rev}}, sum ->
          sum + State.record_bytes(key, value)
        end)

      {:ok, :gb_trees.from_orddict(records), count, bytes}
    end
  end

  defp read_records(<<>>, 0, _last, acc), do: {:ok, Enum.reverse(acc)}

  defp read_records(bytes, count, last, acc) when count > 0 do
    case bytes do
      <<key_len::16, key::binary-size(key_len), revision::64, value_len::32,
        value::binary-size(value_len), rest::binary>>
      when key_len > 0 and (last == nil or key > last) ->
        read_records(rest, count - 1, key, [{key, {value, revision}} | acc])

      _other ->
        :error
    end
  end

  defp read_records(_bytes, _count, _last, _acc), do: :error

  defp decode_sessions({3, count, payload}) do
    with {:ok, sessions} <- read_sessions(payload, count, -1, []) do
      {:ok, :gb_trees.from_orddict(sessions), count}
    end
  end

  defp read_sessions(<<>>, 0, _last, acc), do: {:ok, Enum.reverse(acc)}

  defp read_sessions(bytes, count, last, acc) when count > 0 do
    case bytes do
      <<id::64, lowest::64, seen::64, results::16, rest::binary>> when id > last ->
        read_session(rest, {id, lowest, seen, results}, count, acc)

      _other ->
        :error
    end
  end

  defp read_sessions(_bytes, _count, _last, _acc), do: :error

  defp read_session(bytes, {id, lowest, seen, results}, count, acc) do
    with {:ok, pairs, rest} <- read_results(bytes, results, lowest - 1, []) do
      session = %Session{
        id: id,
        results: :gb_trees.from_orddict(pairs),
        lowest_seq: lowest,
        last_seen_ms: seen
      }

      read_sessions(rest, count - 1, id, [{id, session} | acc])
    end
  end

  defp read_results(bytes, 0, _last, acc), do: {:ok, Enum.reverse(acc), bytes}

  defp read_results(bytes, count, last, acc) do
    case bytes do
      <<seq::64, len::32, encoded::binary-size(len), rest::binary>> when seq > last ->
        read_result(rest, encoded, seq, count, acc)

      _other ->
        :error
    end
  end

  defp read_result(rest, encoded, seq, count, acc) do
    case Result.decode(encoded) do
      {:ok, result} -> read_results(rest, count - 1, seq, [{seq, result} | acc])
      {:error, :malformed_result} -> :error
    end
  end

  defp decode_fields({4, 1, payload}) do
    case payload do
      <<revision::64, data_bytes::64, next_sweep::64>> -> {:ok, revision, data_bytes, next_sweep}
      _other -> :error
    end
  end

  defp check_trailer(frames, {@trailer, 0, payload}, record_count, session_count) do
    payloads = Enum.map(frames, fn {_section, _count, body} -> body end)
    digest = :crypto.hash(:sha256, payloads)

    case payload do
      <<^record_count::64, ^session_count::64, ^digest::binary-size(32)>> -> :ok
      _other -> :error
    end
  end

  defp check_trailer(_frames, _trailer, _records, _sessions), do: :error
end
