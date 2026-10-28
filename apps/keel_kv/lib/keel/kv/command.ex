defmodule Keel.KV.Command do
  @moduledoc "Decoding checks every length before it slices. Kind 7 stays free"

  alias Keel.KV
  alias Keel.KV.Limits

  @format 1
  @kinds [1, 2, 3, 4, 5, 6, 8, 9, 10]

  @fields ~w[client_id seq ack op]a
  @enforce_keys @fields
  defstruct @fields

  @type bound :: nil | KV.key()

  @type op ::
          :register
          | {:put, KV.key(), KV.value()}
          | {:delete, KV.key()}
          | {:compare_and_set, KV.key(), KV.revision(), {:put, KV.value()} | :delete}
          | {:set_limits, %{atom() => non_neg_integer()}}
          | {:get, KV.key()}
          | {:range, KV.key(), bound(), pos_integer()}
          | {:prefix, KV.key(), bound(), pos_integer()}

  @type t :: %__MODULE__{
          client_id: non_neg_integer(),
          seq: non_neg_integer(),
          ack: non_neg_integer(),
          op: op()
        }

  @spec read?(op()) :: boolean()
  def read?({kind, _key}), do: kind == :get
  def read?({kind, _key, _bound, _limit}), do: kind in [:range, :prefix]
  def read?(_op), do: false

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = command) do
    {kind, key, tail} = parts(command.op)
    head = <<@format, kind, command.client_id::64, command.seq::64, command.ack::64>>

    :erlang.iolist_to_binary([head, <<byte_size(key)::16>>, key, tail])
  end

  @spec decode(binary()) :: {:ok, t()} | {:error, :malformed_command}
  def decode(bytes) do
    with {:ok, head, key, tail} <- split(bytes),
         {:ok, op} <- decode_op(head.kind, key, tail) do
      {:ok, %__MODULE__{client_id: head.client_id, seq: head.seq, ack: head.ack, op: op}}
    else
      _other -> {:error, :malformed_command}
    end
  end

  defp parts(:register), do: {1, <<>>, <<>>}
  defp parts({:put, key, value}), do: {2, key, <<byte_size(value)::32, value::binary>>}
  defp parts({:delete, key}), do: {3, key, <<>>}

  defp parts({:compare_and_set, key, expected, {:put, value}}),
    do: {4, key, <<expected::64, byte_size(value)::32, value::binary>>}

  defp parts({:compare_and_set, key, expected, :delete}), do: {5, key, <<expected::64>>}
  defp parts({:set_limits, changes}), do: {6, <<>>, limits_payload(changes)}
  defp parts({:get, key}), do: {8, key, <<>>}
  defp parts({:range, from, to, limit}), do: {9, from, bound_payload(limit, to)}

  defp parts({:prefix, prefix, after_key, limit}),
    do: {10, prefix, bound_payload(limit, after_key)}

  defp limits_payload(changes) do
    pairs = Limits.to_pairs(changes)
    encoded = Enum.map(pairs, fn {id, value} -> <<id, value::64>> end)

    :erlang.iolist_to_binary([<<length(pairs)>> | encoded])
  end

  defp bound_payload(limit, nil), do: <<limit::32, 0>>
  defp bound_payload(limit, bound), do: <<limit::32, 1, byte_size(bound)::16, bound::binary>>

  defp split(bytes) do
    case bytes do
      <<@format, kind, client_id::64, seq::64, ack::64, key_len::16, rest::binary>>
      when kind in @kinds ->
        head = %{kind: kind, client_id: client_id, seq: seq, ack: ack}

        split_key(head, key_len, rest)

      _other ->
        :error
    end
  end

  defp split_key(head, key_len, rest) do
    case rest do
      <<key::binary-size(^key_len), tail::binary>> -> {:ok, head, key, tail}
      _short -> :error
    end
  end

  defp decode_op(1, <<>>, <<>>), do: {:ok, :register}
  defp decode_op(3, key, <<>>), do: {:ok, {:delete, key}}
  defp decode_op(8, key, <<>>), do: {:ok, {:get, key}}
  defp decode_op(6, <<>>, tail), do: decode_limits(tail)

  defp decode_op(2, key, tail) do
    case tail do
      <<len::32, value::binary-size(len)>> -> {:ok, {:put, key, value}}
      _other -> :error
    end
  end

  defp decode_op(4, key, tail) do
    case tail do
      <<expected::64, len::32, value::binary-size(len)>> ->
        {:ok, {:compare_and_set, key, expected, {:put, value}}}

      _other ->
        :error
    end
  end

  defp decode_op(5, key, tail) do
    case tail do
      <<expected::64>> -> {:ok, {:compare_and_set, key, expected, :delete}}
      _other -> :error
    end
  end

  defp decode_op(9, key, tail) do
    with {:ok, limit, to} <- decode_bound(tail), do: {:ok, {:range, key, to, limit}}
  end

  defp decode_op(10, key, tail) do
    with {:ok, limit, after_key} <- decode_bound(tail),
         do: {:ok, {:prefix, key, after_key, limit}}
  end

  defp decode_op(_kind, _key, _tail), do: :error

  defp decode_bound(tail) do
    case tail do
      <<limit::32, 0>> -> {:ok, limit, nil}
      <<limit::32, 1, len::16, bound::binary-size(len)>> -> {:ok, limit, bound}
      _other -> :error
    end
  end

  defp decode_limits(tail) do
    case tail do
      <<count, pairs::binary-size(count * 9)>> -> collect_limits(pairs, %{})
      _other -> :error
    end
  end

  defp collect_limits(<<>>, changes), do: {:ok, {:set_limits, changes}}

  defp collect_limits(pairs, changes) do
    <<id, value::64, rest::binary>> = pairs

    with {:ok, name} <- Limits.name(id),
         false <- Map.has_key?(changes, name) do
      collect_limits(rest, Map.put(changes, name, value))
    else
      _other -> :error
    end
  end
end
