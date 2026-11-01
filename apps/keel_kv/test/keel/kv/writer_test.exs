defmodule Keel.KV.WriterTest do
  use ExUnit.Case, async: true

  alias Keel.KV.Command
  alias Keel.KV.TestUtils

  @moduletag :capture_log

  describe "read/3" do
    test "returns no_leader when nobody answers" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.read_async!(group, timeout: 300)
      reply = Task.await(task)

      assert reply == {:error, :no_leader}
    end

    test "returns the answer of the member without a session" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.read_async!(group)

      assert_receive {:a, read, {:read, {:get, "key"}}}

      TestUtils.reply(read, {:read, {:ok, nil}})
      reply = Task.await(task)

      assert reply == {:ok, nil}
    end

    test "asks again after a refusal" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.read_async!(group)

      assert_receive {:a, first, {:read, _query}}

      TestUtils.reply(first, {:refused, :not_ready})

      assert_receive {:a, second, {:read, _query}}

      TestUtils.reply(second, {:read, {:ok, {"v", 3}}})
      reply = Task.await(task)

      assert reply == {:ok, {"v", 3}}
    end

    test "returns the error of the machine" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.read_async!(group)

      assert_receive {:a, read, {:read, _query}}

      TestUtils.reply(read, {:read, {:error, :invalid_limit}})
      reply = Task.await(task)

      assert reply == {:error, :invalid_limit}
    end
  end

  describe "submit/3" do
    test "returns no_leader when registration never answers" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.put_async!(group, timeout: 200)
      reply = Task.await(task)

      assert reply == {:error, :no_leader}
    end

    test "returns unknown_outcome when the deadline passes after a send" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.put_async!(group, timeout: 400)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})
      reply = Task.await(task)

      assert reply == {:error, :unknown_outcome}
    end

    test "returns unknown_outcome when the session expires" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, put, %Command{op: {:put, _key, _value}}}

      TestUtils.reply(put, {:applied, {:error, :session_expired}})
      reply = Task.await(task)

      assert reply == {:error, :unknown_outcome}
    end

    test "registers again after the session expired" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      first = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, put, %Command{op: {:put, _key, _value}}}

      TestUtils.reply(put, {:applied, {:error, :session_expired}})
      Task.await(first)

      _second = TestUtils.put_async!(group)

      assert_receive {:a, _from, %Command{op: :register}}
    end

    test "returns a failed result as it is" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, put, %Command{op: {:put, _key, _value}}}

      TestUtils.reply(put, {:applied, {:error, :space_exceeded}})
      reply = Task.await(task)

      assert reply == {:error, :space_exceeded}
    end

    test "registers once for many writes" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      first = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, put, %Command{op: {:put, _key, _value}}}

      TestUtils.reply(put, {:applied, {:ok, 8}})
      Task.await(first)

      _second = TestUtils.put_async!(group)

      assert_receive {:a, _from, %Command{client_id: 5, seq: 2}}
      refute_receive {:a, _from, %Command{op: :register}}
    end

    test "follows a redirect to the leader" do
      first = TestUtils.start_relay!(:a)
      second = TestUtils.start_relay!(:b)
      group = TestUtils.start_writer!(a: first, b: second)

      task = TestUtils.put_async!(group)

      assert_receive {:a, redirected, %Command{op: :register}}

      TestUtils.reply(redirected, {:refused, {:not_leader, {"b", :b}}})

      assert_receive {:b, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:b, put, %Command{op: {:put, _key, _value}}}

      TestUtils.reply(put, {:applied, {:ok, 8}})
      reply = Task.await(task)

      assert reply == {:ok, 8}
    end

    test "resends with the same sequence after lost leadership" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, lost, %Command{seq: seq}}

      TestUtils.reply(lost, {:refused, :leadership_lost})

      assert_receive {:a, again, %Command{seq: ^seq}}

      TestUtils.reply(again, {:applied, {:ok, 8}})
      reply = Task.await(task)

      assert reply == {:ok, 8}
    end

    test "resends with the same sequence after an attempt timed out" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!(a: relay)

      task = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, _silent, %Command{seq: seq}}
      assert_receive {:a, again, %Command{seq: ^seq}}

      TestUtils.reply(again, {:applied, {:ok, 8}})
      reply = Task.await(task)

      assert reply == {:ok, 8}
    end

    test "waits instead of resending while the target still answers" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!([a: relay], attempt_timeout_ms: 300)
      slow = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, waiting, %Command{seq: 1}}

      Process.sleep(200)
      quick = TestUtils.put_async!(group)

      assert_receive {:a, answered, %Command{seq: 2}}

      TestUtils.reply(answered, {:applied, {:ok, 2}})

      refute_receive {:a, _again, %Command{seq: 1}}, 200

      TestUtils.reply(waiting, {:applied, {:ok, 1}})
      replies = Enum.map([slow, quick], &Task.await/1)

      assert replies == [{:ok, 1}, {:ok, 2}]
    end

    test "ignores a late reply of an attempt that timed out" do
      relay = TestUtils.start_relay!(:a)
      group = TestUtils.start_writer!([a: relay], attempt_timeout_ms: 300)

      task = TestUtils.put_async!(group)

      assert_receive {:a, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:a, late, %Command{seq: seq}}
      assert_receive {:a, again, %Command{seq: ^seq}}

      TestUtils.reply(late, {:applied, {:ok, 99}})

      yielded = Task.yield(task, 50)

      assert yielded == nil

      TestUtils.reply(again, {:applied, {:ok, 7}})
      reply = Task.await(task)

      assert reply == {:ok, 7}
    end

    test "tries the next node when a server is gone" do
      first = TestUtils.start_relay!(:a)
      second = TestUtils.start_relay!(:b)
      TestUtils.kill!(first)
      group = TestUtils.start_writer!(a: first, b: second)

      task = TestUtils.put_async!(group)

      assert_receive {:b, register, %Command{op: :register}}

      TestUtils.reply(register, {:applied, {:ok, 5}})

      assert_receive {:b, put, %Command{op: {:put, _key, _value}}}

      TestUtils.reply(put, {:applied, {:ok, 8}})
      reply = Task.await(task)

      assert reply == {:ok, 8}
    end
  end
end
