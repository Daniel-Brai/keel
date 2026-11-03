defmodule Keel.KV.CLITest do
  use ExUnit.Case, async: true

  alias Keel.KV.CLI

  describe "plan/1" do
    test "refuses a call without a command" do
      plan = CLI.plan(~w[--node a@host])

      assert {:error, _message} = plan
    end

    test "refuses an unknown option" do
      plan = CLI.plan(~w[get k --node a@host --nope])

      assert plan == {:error, "unknown option --nope"}
    end

    test "refuses a put without a value" do
      plan = CLI.plan(~w[put k --node a@host])

      assert {:error, _message} = plan
    end

    test "refuses a key that is not hex" do
      plan = CLI.plan(~w[get zz --hex --node a@host])

      assert {:error, _message} = plan
    end

    test "decodes a hex key" do
      plan = CLI.plan(~w[get 6b6579 --hex --node a@host])

      assert {:ok, %{request: {:get, ["key"]}}} = plan
    end

    test "decodes a base64 key" do
      plan = CLI.plan(~w[delete a2V5 --base64 --node a@host])

      assert {:ok, %{request: {:delete, ["key"]}}} = plan
    end

    test "reads a short value" do
      plan = CLI.plan(~w[put k --value v --node a@host])

      assert {:ok, %{request: {:put, ~w[k v]}, group: "default"}} = plan
    end
  end

  describe "render/2" do
    test "says a key is missing" do
      lines = CLI.render({:ok, nil}, false)

      assert lines == ["not found"]
    end

    test "hides the value unless asked" do
      lines = CLI.render({:ok, %{value: "hi", revision: 3}}, false)

      assert lines == ["revision 3, 2 bytes"]
    end

    test "shows a value that is not text as hex" do
      lines = CLI.render({:ok, %{value: <<255, 0>>, revision: 3}}, true)

      assert [_head, "ff00"] = lines
    end
  end

  describe "render_status/2" do
    test "calls a timeout unknown" do
      line = CLI.render_status("b", {:error, {:erpc, :timeout}})

      assert line == "b unknown"
    end

    test "calls a lost connection down" do
      line = CLI.render_status("b", {:error, {:erpc, :noconnection}})

      assert line == "b down"
    end
  end
end
