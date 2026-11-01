defmodule Keel.KV.OutcomeTest do
  use ExUnit.Case, async: true

  alias Keel.KV.Outcome

  describe "classify/1" do
    test "ends the call with the answer to a read" do
      action = Outcome.classify({:reply, {:read, {:ok, nil}}})

      assert action == {:done, {:ok, nil}}
    end

    test "ends the call when the session is gone" do
      action = Outcome.classify({:reply, {:applied, {:error, :session_expired}}})

      assert action == :session_lost
    end

    test "treats a refusal as not applied" do
      action = Outcome.classify({:reply, {:refused, :transfer_in_progress}})

      assert {:retry, %{maybe_applied?: false, refusal: :transfer_in_progress}} = action
    end

    test "gives an overloaded refusal straight back" do
      action = Outcome.classify({:reply, {:refused, :overloaded}})

      assert action == {:done, {:error, :overloaded}}
    end

    test "treats a missing server as not applied" do
      action = Outcome.classify({:down, :noproc})

      assert {:retry, %{maybe_applied?: false, rotate?: true}} = action
    end

    test "treats lost leadership as possibly applied" do
      action = Outcome.classify({:reply, {:refused, :leadership_lost}})

      assert {:retry, %{maybe_applied?: true}} = action
    end

    test "treats a timeout as possibly applied" do
      action = Outcome.classify(:timeout)

      assert {:retry, %{maybe_applied?: true, rotate?: true}} = action
    end

    test "treats a lost connection as possibly applied" do
      action = Outcome.classify({:down, :noconnection})

      assert {:retry, %{maybe_applied?: true}} = action
    end

    test "treats a crashed server as possibly applied" do
      action = Outcome.classify({:down, :killed})

      assert {:retry, %{maybe_applied?: true}} = action
    end

    test "waits for a leader when the member knows none" do
      action = Outcome.classify({:reply, {:refused, {:not_leader, nil}}})

      assert {:retry, %{refusal: :no_leader, maybe_applied?: false}} = action
    end

    test "redirects to the leader a member names" do
      action = Outcome.classify({:reply, {:refused, {:not_leader, {"b", :b@host}}}})

      assert action == {:redirect, :b@host}
    end

    test "returns a result that failed as it is" do
      action = Outcome.classify({:reply, {:applied, {:error, {:revision_mismatch, 4}}}})

      assert action == {:done, {:error, {:revision_mismatch, 4}}}
    end

    test "returns a result that worked as it is" do
      action = Outcome.classify({:reply, {:applied, {:ok, 7}}})

      assert action == {:done, {:ok, 7}}
    end
  end
end
