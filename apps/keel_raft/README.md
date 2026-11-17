# keel_raft

The Raft library behind Keel. It keeps the log in the same order on every member of a group. Each committed entry goes to a state machine you supply.

Each member runs on its own BEAM node. A group of three or five members keeps working while a majority is up. An entry commits only when a majority holds it on disk. So an acknowledged entry survives when a minority crashes.

## What is inside

- `Keel.Raft` is the consensus core. It is a pure function from a state and an event to a new state and a list of effects. It touches no clock, disk or network.
- `Keel.Log` is the log store. It writes checksummed records to segment files and syncs many writes at once.
- `Keel.Node` runs one member. It feeds events into the core and carries out the effects that come back.
- `Keel.Transport` sends Raft messages between members over BEAM distribution.
- `Keel.Machine` is the behaviour your state machine implements.

## Using it

A state machine implements five callbacks. `apply/2` must give the same result on every member for the same entry. It never reads a clock or random numbers.

```elixir
defmodule Counter do
  @behaviour Keel.Machine

  @impl Keel.Machine
  def init(_config), do: 0

  @impl Keel.Machine
  def apply(count, _entry), do: {count + 1, {:ok, count + 1}}

  @impl Keel.Machine
  def query(count, :count), do: {:ok, count}

  @impl Keel.Machine
  def snapshot(count), do: <<count::64>>

  @impl Keel.Machine
  def restore(<<count::64>>), do: {:ok, count}
end
```

Start one member of the group on each node. Give every member the same member list and cluster id. The first start of a new cluster sets `bootstrap: true`. Later starts read the log.

```elixir
Keel.start_group(:counter,
  member_id: "a",
  members: %{"a" => :"a@host", "b" => :"b@host", "c" => :"c@host"},
  cluster_id: cluster_id,
  data_dir: "/var/lib/keel/a",
  machine: Counter,
  bootstrap: true
)
```

A new member starts with `join: true` and an empty data directory. Ask the cluster to add it. It catches up as a learner and then votes.

```elixir
Keel.add_member(:counter, "d", :"d@host", :voter)
Keel.remove_member(:counter, "a")
```

To see how one member views the group call `Keel.status/1`. To ask every node at once call `Keel.Cluster.status/1`.

```elixir
Keel.status(:counter)
Keel.Cluster.status(:counter)
```
