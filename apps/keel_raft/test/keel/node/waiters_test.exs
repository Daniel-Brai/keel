defmodule Keel.Node.WaitersTest do
  use ExUnit.Case, async: true

  alias Keel.Node.Waiters

  describe "pop/2" do
    test "returns nil for an unknown reference" do
      waiters = Waiters.new()

      popped = Waiters.pop(waiters, make_ref())

      assert {nil, %{}} = popped
    end

    test "removes the caller" do
      ref = make_ref()
      from = {self(), make_ref()}
      waiters = Waiters.add(Waiters.new(), ref, from)

      {caller, rest} = Waiters.pop(waiters, ref)

      assert caller == from
      size = Waiters.size(rest)

      assert size == 0
    end
  end

  describe "take/2" do
    test "skips references nobody waits on" do
      refs = %{1 => make_ref()}

      {callers, rest} = Waiters.take(Waiters.new(), refs)

      assert callers == %{}
      assert rest == %{}
    end

    test "maps each index to its caller" do
      ref = make_ref()
      from = {self(), make_ref()}
      waiters = Waiters.add(Waiters.new(), ref, from)

      {callers, rest} = Waiters.take(waiters, %{7 => ref})

      assert callers == %{7 => from}
      size = Waiters.size(rest)

      assert size == 0
    end
  end
end
