defmodule Keel.KV.Result do
  @moduledoc "Bytes never come from `term_to_binary` so every member reads the same format"

  alias Keel.KV

  @errors ~w[
    malformed_command
    invalid_key
    key_too_large
    value_too_large
    space_exceeded
    invalid_limits
    invalid_limit
    session_expired
    stale_command
    too_many_pending
  ]a

  @type row :: {KV.key(), KV.value(), KV.revision()}

  @type t ::
          {:ok, nil}
          | {:ok, non_neg_integer()}
          | {:ok, {KV.value(), KV.revision()}}
          | {:ok, {[row()], nil | KV.key()}}
          | {:error, atom()}
          | {:error, {:revision_mismatch, KV.revision()}}

  @spec encode(t()) :: binary()
  def encode({:ok, nil}), do: <<0>>
  def encode({:ok, revision}) when is_integer(revision), do: <<1, revision::64>>

  def encode({:ok, {value, revision}}) when is_binary(value),
    do: <<2, byte_size(value)::32, value::binary, revision::64>>

  def encode({:ok, {records, next}}) do
    encoded = Enum.map(records, &encode_record/1)

    :erlang.iolist_to_binary([<<3, length(records)::32>>, encoded, encode_next(next)])
  end

  def encode({:error, {:revision_mismatch, current}}), do: <<5, current::64>>
  def encode({:error, reason}) when is_atom(reason), do: <<4, error_code(reason)>>

  @spec decode(binary()) :: {:ok, t()} | {:error, :malformed_result}
  def decode(bytes) do
    case parse(bytes) do
      {:ok, result, <<>>} -> {:ok, result}
      _other -> {:error, :malformed_result}
    end
  end

  @spec parse(binary()) :: {:ok, t(), binary()} | :error
  def parse(bytes) do
    case bytes do
      <<0, rest::binary>> -> {:ok, {:ok, nil}, rest}
      <<1, revision::64, rest::binary>> -> {:ok, {:ok, revision}, rest}
      <<2, rest::binary>> -> parse_value(rest)
      <<3, count::32, rest::binary>> -> parse_records(rest, count, [])
      <<4, code, rest::binary>> -> parse_error(code, rest)
      <<5, current::64, rest::binary>> -> {:ok, {:error, {:revision_mismatch, current}}, rest}
      _other -> :error
    end
  end

  defp parse_error(0, _rest), do: :error

  defp parse_error(code, rest) do
    case Enum.at(@errors, code - 1) do
      nil -> :error
      reason -> {:ok, {:error, reason}, rest}
    end
  end

  defp parse_value(bytes) do
    case bytes do
      <<len::32, value::binary-size(len), revision::64, rest::binary>> ->
        {:ok, {:ok, {value, revision}}, rest}

      _other ->
        :error
    end
  end

  defp parse_records(bytes, 0, acc), do: parse_next(bytes, Enum.reverse(acc))

  defp parse_records(bytes, count, acc) do
    case bytes do
      <<key_len::16, key::binary-size(key_len), revision::64, value_len::32,
        value::binary-size(value_len), rest::binary>> ->
        parse_records(rest, count - 1, [{key, value, revision} | acc])

      _other ->
        :error
    end
  end

  defp parse_next(bytes, records) do
    case bytes do
      <<0, rest::binary>> ->
        {:ok, {:ok, {records, nil}}, rest}

      <<1, len::16, next::binary-size(len), rest::binary>> ->
        {:ok, {:ok, {records, next}}, rest}

      _other ->
        :error
    end
  end

  defp encode_record({key, value, revision}) do
    <<byte_size(key)::16, key::binary, revision::64, byte_size(value)::32, value::binary>>
  end

  defp encode_next(nil), do: <<0>>
  defp encode_next(key), do: <<1, byte_size(key)::16, key::binary>>

  defp error_code(reason) do
    case Enum.find_index(@errors, &(&1 == reason)) do
      nil -> raise ArgumentError, "unknown result error #{inspect(reason)}"
      position -> position + 1
    end
  end
end
