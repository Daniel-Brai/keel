defmodule Keel.Log.CodecTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Keel.Log.Codec
  alias Keel.Log.Codec.Header
  alias Keel.Log.Codec.Record
  alias Keel.TestUtils

  @max 1_000_000

  describe "decode_header/1" do
    test "fails for a short header" do
      decoded = Codec.decode_header(<<1, 2, 3>>)

      assert {:error, :short} = decoded
    end

    test "fails for the wrong magic" do
      header = TestUtils.header_bytes(1, 1)
      broken = <<"XEELLOG", 0, binary_part(header, 8, 56)::binary>>

      decoded = Codec.decode_header(broken)

      assert {:error, :bad_magic} = decoded
    end

    property "fails for any flipped bit" do
      check all(
              number <- StreamData.integer(1..99),
              first <- StreamData.integer(1..99),
              max_runs: 20
            ) do
        header = TestUtils.header_bytes(number, first)
        flips = TestUtils.undetected_header_flips(header)

        assert flips == []
      end
    end

    test "decodes what encode_header wrote" do
      header = TestUtils.header_bytes(3, 9)

      decoded = Codec.decode_header(header)

      assert {:ok, %Header{segment: 3, first_seq: 9}} = decoded
    end
  end

  describe "decode_record/3" do
    test "fails for a length over the limit" do
      record = TestUtils.raw_record(3, <<0::64>>, 1, 1)

      decoded = Codec.decode_record(record, 1, 4)

      assert {:error, :too_long} = decoded
    end

    test "fails for another segment" do
      record = Codec.encode_record({:truncate, 4}, 1, 2)

      decoded = Codec.decode_record(record, 1, @max)

      assert {:error, :wrong_segment} = decoded
    end

    test "fails for an unknown type" do
      record = TestUtils.raw_record(9, <<0::64>>, 1, 1)

      decoded = Codec.decode_record(record, 1, @max)

      assert {:error, :unknown_type} = decoded
    end

    test "fails for a payload of the wrong shape" do
      record = TestUtils.raw_record(3, <<0::32>>, 1, 1)

      decoded = Codec.decode_record(record, 1, @max)

      assert {:error, :bad_payload} = decoded
    end

    test "fails for an entry count the payload cannot hold" do
      record = TestUtils.raw_record(1, <<4_000_000_000::32, 1, 2, 3>>, 1, 1)

      decoded = Codec.decode_record(record, 1, @max)

      assert {:error, :bad_payload} = decoded
    end

    test "fails for flags that are set" do
      record = TestUtils.raw_record(3, <<5::64>>, 1, 1, 1)

      decoded = Codec.decode_record(record, 1, @max)

      assert {:error, :bad_flags} = decoded
    end

    test "returns end for a zero header" do
      decoded = Codec.decode_record(<<0::size(400)>>, 1, @max)

      assert :end = decoded
    end

    test "returns the bytes after the record" do
      record = Codec.encode_record({:truncate, 4}, 7, 1)

      input = <<record::binary, "rest">>

      decoded = Codec.decode_record(input, 1, @max)

      assert {:ok, %Record{seq: 7}, "rest"} = decoded
    end

    property "decodes what encode_record wrote" do
      check all(
              body <- TestUtils.random_record(),
              seq <- StreamData.integer(1..9_999),
              max_runs: 200
            ) do
        encoded = Codec.encode_record(body, seq, 5)

        decoded = Codec.decode_record(encoded, 5, @max)

        assert {:ok, %Record{body: ^body, seq: ^seq}, <<>>} = decoded
      end
    end

    property "reports every truncation as short" do
      check all(body <- TestUtils.random_record(), max_runs: 100) do
        encoded = Codec.encode_record(body, 3, 5)
        wrong = TestUtils.wrong_truncations(encoded, 5)

        assert wrong == []
      end
    end

    property "detects every single bit flip" do
      check all(body <- TestUtils.random_record(), max_runs: 60) do
        encoded = Codec.encode_record(body, 3, 5)
        flips = TestUtils.undetected_flips(encoded, 5)

        assert flips == []
      end
    end

    property "never raises on random bytes" do
      check all(bytes <- StreamData.binary(max_length: 200), max_runs: 300) do
        class = TestUtils.decode_class(bytes)

        assert class in ~w[record end error]a
      end
    end
  end
end
