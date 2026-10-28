defmodule Keel.KV.CommandTest do
  use ExUnit.Case, async: true

  alias Keel.KV.Command
  alias Keel.KV.TestUtils

  describe "decode/1" do
    test "fails on empty bytes" do
      decoded = Command.decode(<<>>)

      assert {:error, :malformed_command} = decoded
    end

    test "fails on an unknown kind" do
      bytes = <<1, 7, 1::64, 1::64, 1::64, 0::16>>

      decoded = Command.decode(bytes)

      assert {:error, :malformed_command} = decoded
    end

    test "fails on an unknown format" do
      bytes = <<2, 1, 1::64, 1::64, 1::64, 0::16>>

      decoded = Command.decode(bytes)

      assert {:error, :malformed_command} = decoded
    end

    test "fails when the key is longer than the bytes" do
      bytes = <<1, 3, 1::64, 1::64, 1::64, 9::16, "key">>

      decoded = Command.decode(bytes)

      assert {:error, :malformed_command} = decoded
    end

    test "fails on bytes after the command" do
      command = %Command{client_id: 1, seq: 1, ack: 1, op: {:delete, "key"}}
      bytes = Command.encode(command)

      decoded = Command.decode(bytes <> "x")

      assert {:error, :malformed_command} = decoded
    end

    test "fails on a repeated limit" do
      pair = <<1, 5::64>>
      bytes = <<1, 6, 1::64, 1::64, 1::64, 0::16, 2, pair::binary, pair::binary>>

      decoded = Command.decode(bytes)

      assert {:error, :malformed_command} = decoded
    end

    test "fails on an unknown limit" do
      bytes = <<1, 6, 1::64, 1::64, 1::64, 0::16, 1, 99, 5::64>>

      decoded = Command.decode(bytes)

      assert {:error, :malformed_command} = decoded
    end

    test "fails on every cut of a command" do
      command = %Command{
        client_id: 1,
        seq: 1,
        ack: 1,
        op: {:compare_and_set, "key", 3, {:put, "value"}}
      }

      bytes = Command.encode(command)

      cuts = TestUtils.decoding_prefixes(bytes)

      assert cuts == []
    end

    test "returns a command of every kind" do
      failures = TestUtils.roundtrip_failures(TestUtils.ops())

      assert failures == []
    end
  end

  describe "read?/1" do
    test "fails for a write" do
      read? = Command.read?({:put, "a", "b"})

      refute read?
    end

    test "fails for a delete" do
      read? = Command.read?({:delete, "a"})

      refute read?
    end

    test "fails for a register" do
      read? = Command.read?(:register)

      refute read?
    end

    test "passes a get" do
      read? = Command.read?({:get, "a"})

      assert read?
    end

    test "passes a range" do
      read? = Command.read?({:range, "a", nil, 1})

      assert read?
    end

    test "passes a prefix scan" do
      read? = Command.read?({:prefix, "a", nil, 1})

      assert read?
    end
  end
end
