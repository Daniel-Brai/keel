# keel_kv

A replicated key-value store built on `keel_raft`. Every member holds a full copy of the data. Reads and writes are linearizable. A read sees every write that finished before it started.

## What it offers

- Put, get and delete of binary keys up to 1 KiB and values up to 1 MiB.
- Compare and set on a key's revision. The revision is the log index of the key's last write. A revision of 0 means the key does not exist.
- Range and prefix scans, paged by key.
- Safe retries. Each node keeps one client session. A write that times out and is sent again never applies twice.

The data must fit in memory. Snapshots keep the log on disk short.

## Using it

On each node start a group with `Keel.KV.Machine`. Then start the writer that holds the client session for that node.

```elixir
Keel.start_group(:kv,
  member_id: "a",
  members: %{"a" => :"a@host", "b" => :"b@host", "c" => :"c@host"},
  cluster_id: cluster_id,
  data_dir: "/var/lib/keel/a",
  machine: Keel.KV.Machine
)

Keel.KV.start_writer(:kv, [])
```

A call on any node finds the leader by itself.

```elixir
{:ok, revision} = Keel.KV.put(:kv, "user/1", "ada", [])
{:ok, %{value: "ada"}} = Keel.KV.get(:kv, "user/1", [])
{:ok, _revision} = Keel.KV.compare_and_set(:kv, "user/1", revision, {:put, "grace"}, [])
```

The errors `:timeout`, `:leadership_lost` and `:unknown_outcome` mean the write may have been applied. Read the key before you decide what to do.

## From the shell

The mix tasks connect to a running node.

```sh
mix keel.kv put user/1 --value ada --group kv --node a@host
mix keel.kv get user/1 --group kv --node a@host
mix keel.status --group kv --node a@host
```
