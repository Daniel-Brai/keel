defmodule Keel.KVTest do
  use ExUnit.Case, async: true

  alias Keel.KV
  alias Keel.KV.Command
  alias Keel.KV.Result

  describe "decode_result/1" do
    test "fails on bytes that are not a result" do
      decoded = KV.decode_result("junk")

      assert {:error, :malformed_result} = decoded
    end

    test "returns the result that was encoded" do
      bytes = Result.encode({:ok, {"value", 4}})

      decoded = KV.decode_result(bytes)

      assert {:ok, {:ok, {"value", 4}}} = decoded
    end
  end

  describe "request/2" do
    test "puts the session in the command" do
      session = %{client_id: 5, seq: 2, ack: 1}

      bytes = KV.request(session, {:put, "a", "1"})

      assert {:ok, %Command{client_id: 5, seq: 2, ack: 1, op: {:put, "a", "1"}}} =
               Command.decode(bytes)
    end
  end

  describe "register_request/0" do
    test "asks for a session" do
      bytes = KV.register_request()

      decoded = Command.decode(bytes)

      assert {:ok, %Command{client_id: 0, op: :register}} = decoded
    end
  end
end
