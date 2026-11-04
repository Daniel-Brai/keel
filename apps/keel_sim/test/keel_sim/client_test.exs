defmodule KeelSim.ClientTest do
  use ExUnit.Case, async: true

  alias Keel.KV
  alias Keel.KV.Command
  alias KeelSim.Client
  alias KeelSim.Client.Op
  alias KeelSim.TestUtils

  describe "step/3" do
    test "registers before anything else" do
      client = Client.new(1)

      {_client, outputs} = Client.step(client, 100, {:put, {:own, 0}, "m1"})

      request = KV.register_request()

      assert outputs == [{:send, "m1", request, 0}]
    end

    test "waits before its deadline" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {_client, outputs} = Client.step(client, 200, {:get, {:own, 0}, "m2"})

      assert outputs == []
    end

    test "sends again with the same sequence number after its deadline" do
      {client, [_invoke, {:send, "m1", data, 1}]} =
        TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {_client, outputs} = Client.step(client, 4_000, {:get, {:own, 0}, "m2"})

      assert outputs == [{:send, "m2", data, 1}]
    end

    test "ends as unknown after its last try and starts a new process" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})
      {second, _outputs} = Client.step(client, 4_000, {:get, {:own, 0}, "m2"})
      {third, _outputs} = Client.step(second, 8_000, {:get, {:own, 0}, "m2"})

      {ended, outputs} = Client.step(third, 12_000, {:get, {:own, 0}, "m2"})

      assert [{:event, {:info, 12_000, %Op{process: 1}}}] = outputs
      assert ended.generation == 1
    end

    test "starts a put with a value that carries its checksum" do
      {_client, [{:event, {:invoke, 100, op}}, _send]} =
        TestUtils.pending_client({:put, {:own, 0}, "m1"})

      checked = KeelSim.KvCheck.valid_value?(op.value)

      assert {op.kind, op.key, checked} == {:put, "p/1/0", true}
    end

    test "sends the command in its session" do
      {_client, [_invoke, {:send, "m1", data, 1}]} =
        TestUtils.pending_client({:put, {:shared, 2}, "m1"})

      assert {:ok, %Command{client_id: 7, seq: 1, ack: 1, op: {:put, "s/2", _value}}} =
               Command.decode(data)
    end

    test "asks the member to read without a command" do
      {_client, [_invoke, query]} = TestUtils.pending_client({:get, {:shared, 2}, "m1"})

      assert query == {:query, "m1", {:get, "s/2"}, 1}
    end
  end

  describe "reply/4" do
    test "ignores a reply for an older sequence number" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {same, outputs} = Client.reply(client, 0, {:result, {:ok, 5}}, 150)

      assert {outputs, same} == {[], client}
    end

    test "registers the session from the result" do
      client = Client.new(1)
      {waiting, _outputs} = Client.step(client, 100, {:put, {:own, 0}, "m1"})

      {registered, _outputs} = Client.reply(waiting, 0, {:result, {:ok, 12}}, 150)

      assert {registered.session, registered.pending} == {12, nil}
    end

    test "ends as failed when the first try was refused with no leader known" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {_client, outputs} = Client.reply(client, 1, {:refused, {:not_leader, nil}}, 150)

      assert [{:event, {:fail, 150, %Op{status: :fail}}}] = outputs
    end

    test "follows a redirect to the leader" do
      {client, [_invoke, {:send, "m1", data, 1}]} =
        TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {_client, outputs} = Client.reply(client, 1, {:refused, {:not_leader, "m3"}}, 150)

      assert outputs == [{:send, "m3", data, 1}]
    end

    test "tries again at once after leadership was lost" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})
      {lost, _outputs} = Client.reply(client, 1, {:refused, :leadership_lost}, 150)

      {_client, outputs} = Client.step(lost, 150, {:put, {:own, 0}, "m2"})

      assert [{:send, "m2", _data, 1}] = outputs
    end

    test "ends as ok with the result of a write" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {done, outputs} = Client.reply(client, 1, {:result, {:ok, 9}}, 150)

      assert [{:event, {:ok, 150, %Op{result: {:ok, 9}}}}] = outputs
      assert done.revisions == %{"p/1/0" => 9}
    end

    test "ends as ok when a compare and set found another revision" do
      {client, _outputs} = TestUtils.pending_client({:compare_and_set, {:shared, 0}, "m1"})
      result = {:error, {:revision_mismatch, 14}}

      {done, outputs} = Client.reply(client, 1, {:result, result}, 150)

      assert [{:event, {:ok, 150, %Op{result: ^result}}}] = outputs
      assert done.revisions == %{"s/0" => 14}
    end

    test "ends as failed when an expired session answers the first try" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {done, outputs} = Client.reply(client, 1, {:result, {:error, :session_expired}}, 150)

      assert [{:event, {:fail, 150, %Op{}}}] = outputs
      assert done.session == nil
    end

    test "ends as unknown when an expired session answers a retry" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})
      {retried, _outputs} = Client.step(client, 4_000, {:get, {:own, 0}, "m2"})

      {done, outputs} = Client.reply(retried, 1, {:result, {:error, :session_expired}}, 4_100)

      assert [{:event, {:info, 4_100, %Op{}}}] = outputs
      assert done.session == nil
    end
  end

  describe "finish/2" do
    test "ends a pending command as unknown" do
      {client, _outputs} = TestUtils.pending_client({:put, {:own, 0}, "m1"})

      {_client, outputs} = Client.finish(client, 900)

      assert [{:event, {:info, 900, %Op{status: :info}}}] = outputs
    end
  end
end
