defmodule Mix.Tasks.Keel.Sim do
  @shortdoc "Runs simulated clusters from seeds and prints violations and coverage"

  use Mix.Task
  use Boundary, classify_to: KeelSim

  alias KeelSim.Coverage
  alias KeelSim.Schedule
  alias KeelSim.Shrink
  alias KeelSim.Trace

  @switches [
    seed: :integer,
    seeds: :integer,
    steps: :integer,
    size: :integer,
    bug: :string,
    workload: :string,
    replay: :string,
    shrink: :boolean,
    save: :string,
    trace: :string
  ]

  @bugs %{
    "double_vote" => :double_vote,
    "ack_before_durable" => :ack_before_durable,
    "commit_old_term" => :commit_old_term,
    "keep_log_after_snapshot" => :keep_log_after_snapshot,
    "read_without_confirm" => :read_without_confirm,
    "no_pre_vote" => :no_pre_vote,
    "change_without_term_commit" => :change_without_term_commit,
    "kv_drop_put" => :kv_drop_put,
    "kv_no_dedupe" => :kv_no_dedupe
  }

  @workloads %{"chain" => :chain, "kv" => :kv}

  @impl Mix.Task
  def run(args) do
    {opts, _rest} = OptionParser.parse!(args, strict: @switches)
    results = results(opts)

    report(results, opts)
  end

  defp results(opts) do
    cond do
      opts[:replay] -> replay(opts)
      opts[:trace] -> traced(opts)
      true -> sweep(opts)
    end
  end

  # One short run that logs every core step as a line of JSON
  defp traced(opts) do
    path = Keyword.fetch!(opts, :trace)
    seed = Keyword.get(opts, :seed, 1)
    steps = Keyword.get(opts, :steps, 150)
    run_opts = Trace.options(seed, steps)
    result = KeelSim.run(run_opts)
    text = Trace.encode(result.config, result.trace)

    File.write!(path, text)
    Mix.shell().info("wrote #{length(result.trace)} lines to #{path}")

    [result]
  end

  defp replay(opts) do
    path = Keyword.fetch!(opts, :replay)

    case Schedule.load(path) do
      {:ok, %{config: config, schedule: schedule}} -> [KeelSim.replay(config, schedule)]
      {:error, :unreadable} -> Mix.raise("cannot read a schedule from #{path}")
    end
  end

  defp sweep(opts) do
    first = Keyword.get(opts, :seed, 1)
    count = Keyword.get(opts, :seeds, 1)
    last = first + count - 1
    run_opts = run_options(opts)

    first..last
    |> Task.async_stream(&run_seed(run_opts, &1), timeout: :infinity)
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp run_seed(run_opts, seed) do
    seeded = Map.put(run_opts, :seed, seed)

    KeelSim.run(seeded)
  end

  defp run_options(opts) do
    base = %{steps: Keyword.get(opts, :steps, 5_000)}
    sized = if opts[:size], do: Map.put(base, :size, opts[:size]), else: base

    loaded =
      if opts[:workload], do: Map.put(sized, :workload, workload!(opts[:workload])), else: sized

    bug = opts[:bug]

    if bug, do: Map.put(loaded, :bug, bug!(bug)), else: loaded
  end

  defp workload!(name) do
    case Map.fetch(@workloads, name) do
      {:ok, workload} -> workload
      :error -> Mix.raise("unknown workload #{name}")
    end
  end

  defp bug!(name) do
    case Map.fetch(@bugs, name) do
      {:ok, bug} -> bug
      :error -> Mix.raise("unknown bug #{name}")
    end
  end

  defp report(results, opts) do
    Enum.each(results, &print_run/1)

    failing = Enum.filter(results, &failing?/1)
    message = "#{length(failing)} of #{length(results)} runs failed"

    print_coverage(results)
    print_failures(failing, opts)

    if failing != [], do: Mix.raise(message)
  end

  defp failing?(result), do: result.violations != [] or result.liveness != []

  defp print_run(result) do
    hash = Base.encode16(result.trace_hash, case: :lower)
    status = if failing?(result), do: "FAIL", else: "ok"
    seed = result.config.seed
    size = result.config.size
    workload = result.config.workload
    ops = length(result.history)
    head = "seed #{seed} size #{size} #{workload} ops #{ops} steps #{result.steps}"
    line = "#{head} heal #{result.heal_steps} trace #{hash} #{status}"

    Mix.shell().info(line)
  end

  defp print_coverage(results) do
    totals = Enum.reduce(results, Coverage.new(), &Coverage.merge(&2, &1.coverage))
    missing = Coverage.missing(totals)
    sorted = Enum.sort(totals)

    Mix.shell().info("coverage")
    Enum.each(sorted, fn {name, count} -> Mix.shell().info("  #{name} #{count}") end)

    if missing != [], do: Mix.shell().info("never reached #{inspect(missing)}")
  end

  defp print_failures([], _opts), do: :ok

  defp print_failures([first | _rest] = failing, opts) do
    Enum.each(failing, &print_failure/1)

    schedule = if opts[:shrink], do: shrunk(first), else: first.schedule

    if opts[:save], do: Schedule.save(opts[:save], first.config, schedule)
  end

  defp print_failure(result) do
    seed = result.config.seed
    violations = inspect(result.violations)
    liveness = inspect(result.liveness)

    Mix.shell().info("seed #{seed} violations #{violations}")
    Mix.shell().info("seed #{seed} liveness #{liveness}")
  end

  defp shrunk(result) do
    class = Shrink.class(result)
    schedule = Shrink.shrink(result.config, result.schedule, class)
    before = length(result.schedule)
    after_count = length(schedule)
    text = inspect(schedule, limit: :infinity, pretty: true)

    Mix.shell().info("shrunk #{before} to #{after_count} actions")
    Mix.shell().info(text)

    schedule
  end
end
