defmodule Keel.KV.ResultTest do
  use ExUnit.Case, async: true

  alias Keel.KV.Result
  alias Keel.KV.TestUtils

  describe "decode/1" do
    test "fails on empty bytes" do
      decoded = Result.decode(<<>>)

      assert {:error, :malformed_result} = decoded
    end

    test "fails on an unknown tag" do
      decoded = Result.decode(<<9>>)

      assert {:error, :malformed_result} = decoded
    end

    test "fails on an unknown error code" do
      decoded = Result.decode(<<4, 0>>)

      assert {:error, :malformed_result} = decoded
    end

    test "fails on bytes after the result" do
      bytes = Result.encode({:ok, 7})

      decoded = Result.decode(bytes <> "x")

      assert {:error, :malformed_result} = decoded
    end

    test "returns a result of every shape" do
      failures = TestUtils.result_failures(TestUtils.results())

      assert failures == []
    end
  end
end
