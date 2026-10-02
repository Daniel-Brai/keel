defmodule KeelSim do
  @moduledoc "The same seed and options always give the same trace hash"

  use Boundary,
    deps: [Keel.Raft, Keel.Chain, Keel.Env, Keel.KV, Keel.Log, Keel.Machine],
    exports: [
      Client,
      Cluster,
      Coverage,
      Disk,
      History,
      Invariants,
      KvCheck,
      Linearizable,
      Liveness,
      Network,
      Schedule,
      Shrink,
      Swarm,
      Trace
    ]

  alias KeelSim.Cluster

  @doc "Options that are left out come from the seed."
  @spec run(Cluster.opts()) :: Cluster.result()
  def run(opts), do: opts |> Cluster.new() |> Cluster.run()

  @doc "Actions that are not enabled are skipped. The heal phase follows unless `heal?` is false."
  @spec replay(Cluster.opts(), [Cluster.action()], boolean()) :: Cluster.result()
  def replay(opts, schedule, heal? \\ true),
    do: opts |> Cluster.new() |> Cluster.run_schedule(schedule, heal?)
end
