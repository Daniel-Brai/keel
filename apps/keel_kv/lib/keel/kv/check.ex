defmodule Keel.KV.Check do
  alias Keel.KV
  alias Keel.KV.Limits

  @type error :: :invalid_key | :key_too_large | :value_too_large

  @spec key(term()) :: :ok | {:error, error()}
  def key(<<>>), do: {:error, :invalid_key}

  def key(key) when is_binary(key) do
    limit = Limits.defaults().max_key_bytes

    if byte_size(key) > limit, do: {:error, :key_too_large}, else: :ok
  end

  def key(_key), do: {:error, :invalid_key}

  @spec value(term()) :: :ok | {:error, error()}
  def value(value) when is_binary(value) do
    limit = Limits.defaults().max_value_bytes

    if byte_size(value) > limit, do: {:error, :value_too_large}, else: :ok
  end

  def value(_value), do: {:error, :value_too_large}

  @spec change(KV.change() | term()) :: :ok | {:error, error()}
  def change(:delete), do: :ok
  def change({:put, value}), do: value(value)
  def change(_change), do: {:error, :value_too_large}
end
