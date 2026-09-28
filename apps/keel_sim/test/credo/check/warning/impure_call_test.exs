defmodule Credo.Check.Warning.ImpureCallTest do
  use Credo.Test.Case, async: true

  @described_check Credo.Check.Warning.ImpureCall

  describe "run/2" do
    test "reports reading the clock" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def now, do: System.monotonic_time()
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{line_no: 2, trigger: "System.monotonic_time"})
    end

    test "reports reading the clock in the key value snapshot" do
      source = ~S'''
      defmodule Keel.KV.Snapshot do
        def stamp, do: System.os_time()
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: "System.os_time"})
    end

    test "reports reading the clock through erlang" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def now, do: :erlang.monotonic_time()
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: ":erlang.monotonic_time"})
    end

    test "reports sending a message" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def notify(pid), do: send(pid, :hello)
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: "send"})
    end

    test "reports receiving a message" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def wait do
          receive do
            message -> message
          end
        end
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: "receive"})
    end

    test "reports randomness from the process dictionary" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def draw, do: :rand.uniform(10)
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: ":rand.uniform"})
    end

    test "reports a shared table" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def find(key), do: :ets.lookup(:table, key)
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: ":ets.lookup"})
    end

    test "reports logging" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def note(text), do: Logger.info(text)
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: "Logger.info"})
    end

    test "reports reading config" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def limit, do: Application.get_env(:keel_raft, :limit)
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: "Application.get_env"})
    end

    test "reports a new reference" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def ref, do: make_ref()
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      assert_issue(issues, %{trigger: "make_ref"})
    end

    test "allows explicit random state" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        def draw(state), do: :rand.uniform_s(10, state)
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      refute_issues(issues)
    end

    test "allows types that name impure modules" do
      source = ~S'''
      defmodule Keel.Raft.Sample do
        @type state :: :rand.state()
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      refute_issues(issues)
    end

    test "allows impure calls outside the pure modules" do
      source = ~S'''
      defmodule Keel.Node.Server do
        def now, do: System.monotonic_time()
      end
      '''

      issues = source |> to_source_file() |> run_check(@described_check)

      refute_issues(issues)
    end
  end
end
