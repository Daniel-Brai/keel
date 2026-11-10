defmodule KeelSim.TraceTest do
  use ExUnit.Case, async: true

  alias KeelSim.Trace

  describe "options/2" do
    test "gives the same trace for the same seed" do
      options = Trace.options(7, 80)

      first = KeelSim.run(options)
      second = KeelSim.run(options)

      assert first.trace == second.trace
    end

    test "logs a line for every message and persist" do
      options = Trace.options(3, 150)
      result = KeelSim.run(options)

      events = Enum.map(result.trace, & &1["ev"])

      assert "message" in events
      assert "persisted" in events
      assert "tick" in events
    end

    test "holds the log terms and values the model compares" do
      options = Trace.options(3, 150)
      result = KeelSim.run(options)

      lines = Enum.filter(result.trace, &(&1["log"] != []))
      last = List.last(lines)
      entry = hd(last["log"])

      assert entry |> Map.keys() |> Enum.sort() == ~w[term value]
    end
  end

  describe "encode/2" do
    test "starts with a header and holds one line of JSON per step" do
      options = Trace.options(5, 100)
      result = KeelSim.run(options)

      text = Trace.encode(result.config, result.trace)
      rows = String.split(text, "\n", trim: true)
      [first | _rest] = rows
      header = JSON.decode!(first)

      assert header == %{"ev" => "init", "size" => 3, "pre_vote" => result.config.pre_vote}
      assert length(rows) == length(result.trace) + 1
    end
  end
end
