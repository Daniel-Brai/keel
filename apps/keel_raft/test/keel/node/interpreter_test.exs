defmodule Keel.Node.InterpreterTest do
  use ExUnit.Case, async: true

  alias Keel.TestUtils

  describe "run/2" do
    test "covers every kind of effect the core returns" do
      lists = TestUtils.recorded_effects()

      kinds = TestUtils.effect_kinds(lists)

      assert kinds == ~w[
             apply
             compact_log
             install_snapshot
             persist
             reply
             role_changed
             send
             store_snapshot_chunk
             take_snapshot
             truncate_from
           ]a
    end

    test "sends each effect to its port in the order returned" do
      lists = TestUtils.recorded_effects()

      mismatches = TestUtils.port_mismatches(lists)

      assert mismatches == []
    end
  end
end
