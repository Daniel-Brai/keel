defmodule KeelSim.PurityTest do
  use ExUnit.Case, async: true

  alias KeelSim.Purity
  alias KeelSim.TestUtils

  describe "violations/1" do
    test "fails a pure module that calls an impure helper" do
      modules = TestUtils.compiled_imports(TestUtils.impure_helper_fixture())

      violations = Purity.violations(modules)

      assert violations == [{Keel.Raft.PurityFixture, {Keel.PurityFixture.Clock, :now, 0}}]
    end

    test "fails process dictionary randomness" do
      modules = [{Keel.Raft.Fixture, [{:rand, :uniform, 1}]}]

      violations = Purity.violations(modules)

      assert [{Keel.Raft.Fixture, {:rand, :uniform, 1}}] = violations
    end

    test "fails time from the erlang module" do
      modules = [{Keel.Raft.Fixture, [{:erlang, :monotonic_time, 0}]}]

      violations = Purity.violations(modules)

      assert [{Keel.Raft.Fixture, {:erlang, :monotonic_time, 0}}] = violations
    end

    test "ignores modules outside the pure set" do
      modules = [{Keel.Node.Server, [{:erlang, :monotonic_time, 0}]}]

      violations = Purity.violations(modules)

      assert violations == []
    end

    test "fails the key value command codec on a clock call" do
      modules = [{Keel.KV.Command, [{:erlang, :system_time, 0}]}]

      violations = Purity.violations(modules)

      assert [{Keel.KV.Command, {:erlang, :system_time, 0}}] = violations
    end

    test "passes the real key value machine" do
      modules = TestUtils.compiled_app_imports(:keel_kv)

      violations = Purity.violations(modules)

      assert violations == []
    end

    test "passes the real core" do
      modules = TestUtils.compiled_app_imports(:keel_raft)

      violations = Purity.violations(modules)

      assert violations == []
    end
  end
end
