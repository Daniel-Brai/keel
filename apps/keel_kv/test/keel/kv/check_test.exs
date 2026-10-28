defmodule Keel.KV.CheckTest do
  use ExUnit.Case, async: true

  alias Keel.KV.Check

  describe "key/1" do
    test "refuses an empty key" do
      checked = Check.key(<<>>)

      assert checked == {:error, :invalid_key}
    end

    test "refuses a key past the limit" do
      key = :binary.copy("k", 2_000)

      checked = Check.key(key)

      assert checked == {:error, :key_too_large}
    end

    test "accepts a short key" do
      checked = Check.key("name")

      assert checked == :ok
    end
  end

  describe "value/1" do
    test "refuses a value past the limit" do
      value = :binary.copy("v", 1_048_577)

      checked = Check.value(value)

      assert checked == {:error, :value_too_large}
    end

    test "accepts an empty value" do
      checked = Check.value(<<>>)

      assert checked == :ok
    end
  end

  describe "change/1" do
    test "checks the value of a put" do
      value = :binary.copy("v", 1_048_577)

      checked = Check.change({:put, value})

      assert checked == {:error, :value_too_large}
    end

    test "accepts a delete" do
      checked = Check.change(:delete)

      assert checked == :ok
    end
  end
end
