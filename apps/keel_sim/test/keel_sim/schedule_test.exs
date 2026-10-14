defmodule KeelSim.ScheduleTest do
  use ExUnit.Case, async: true

  alias KeelSim.Schedule

  describe "load/1" do
    @tag :tmp_dir
    test "fails for a missing file", %{tmp_dir: dir} do
      path = Path.join(dir, "missing.sched")

      loaded = Schedule.load(path)

      assert loaded == {:error, :unreadable}
    end

    @tag :tmp_dir
    test "fails for a file that holds something else", %{tmp_dir: dir} do
      path = Path.join(dir, "other.sched")
      File.write!(path, "not a schedule")

      loaded = Schedule.load(path)

      assert loaded == {:error, :unreadable}
    end

    @tag :tmp_dir
    test "returns what save wrote", %{tmp_dir: dir} do
      path = Path.join(dir, "run.sched")
      result = KeelSim.run(%{seed: 6, size: 3, steps: 300})
      Schedule.save(path, result.config, result.schedule)

      loaded = Schedule.load(path)

      assert loaded == {:ok, %{config: result.config, schedule: result.schedule}}
    end
  end

  describe "replaying a saved run" do
    @tag :tmp_dir
    test "gives the trace of the run", %{tmp_dir: dir} do
      path = Path.join(dir, "run.sched")
      result = KeelSim.run(%{seed: 6, size: 3, steps: 600})
      Schedule.save(path, result.config, result.schedule)
      {:ok, saved} = Schedule.load(path)

      replayed = KeelSim.replay(saved.config, saved.schedule)

      assert replayed.trace_hash == result.trace_hash
    end
  end
end
