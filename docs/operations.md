# Running Keel

This is what Keel 0.1 can do today. Every call below exists in the code. Run the calls on a member's node. From a release use `bin/keel rpc '...'`. On a dev node use `iex`.

## Start the release

Build it with `MIX_ENV=prod mix release keel`. Start each member with its own node name and the same cookie.

```sh
RELEASE_DISTRIBUTION=name RELEASE_NODE=a@10.0.0.1 RELEASE_COOKIE=secret bin/keel daemon
```

The release starts the application and nothing else. It does not start a group. Your service wrapper must call `Keel.start_group/2` after every boot.

## Bootstrap a group

Pick 16 random bytes once and keep them. They are the cluster id. Every member must get the same bytes now and on every restart.

On each member run the call below. Use that member's own `member_id` and `data_dir`.

```elixir
Keel.start_group(:default,
  member_id: "a",
  members: %{"a" => :"a@10.0.0.1", "b" => :"b@10.0.0.2", "c" => :"c@10.0.0.3"},
  cluster_id: cluster_id,
  data_dir: "/var/lib/keel/a",
  machine: Keel.KV.Machine,
  bootstrap: true
)
```

An empty data directory with no `bootstrap: true` and no `join: true` returns `{:error, :not_bootstrapped}`. A directory that already holds a log ignores `bootstrap`. So a restart uses the same call. Use 3, 5 or 7 members. They survive one, two or three failures.

Options go in `raft: [...]`. These matter most.

- `tick_ms` (50)
- `election_ticks_min` (10)
- `election_ticks_max` (20)
- `heartbeat_ticks` (2)
- `snapshot_threshold_bytes` (256 MiB)

Give every member the same values.

## Read the status

On any member:

```elixir
Keel.Cluster.status(:default, timeout: 2_000)
```

It asks every member in the configuration. It returns `%{member_id => {:ok, status} | {:error, reason}}`. A member that doesn't answer shows as an error. It doesn't slow the rest. `Keel.status(:default)` returns only the local member.

A status holds `role`, `term`, `leader`, `commit`, `applied`, `first_index`, `last_index`, `dropped`, `voters` and `learners`. Two leaders in one term is a safety bug. Two leaders in different terms are normal for a moment. From a dev checkout `mix keel.status --node a@10.0.0.1` prints one line per member.

## Add a member

1. Start the new node with an empty data directory. Call `Keel.start_group/2` with `join: true`. Give it the same `cluster_id` and a `members` map of the nodes it can reach. Include itself.
2. On any member run `Keel.add_member(:default, "d", :"d@10.0.0.4", :learner)`. A learner gets the log or a snapshot. It doesn't vote.
3. Watch its `applied` index in the status until it is close to the leader's `commit`.
4. Run `Keel.promote_member(:default, "d")`. It is refused with `:not_caught_up` until the learner is within `promote_lag_entries` (1,000) of the leader. The call retries for about ten seconds.

`Keel.add_member(:default, "d", node, :voter)` does steps 2 and 4 in one call.

## Remove a member

Run `Keel.remove_member(:default, "d")` on any member. A leader that removes itself keeps leading until the removal commits. Then it steps down. The last voter can't be removed. You get `:last_voter`.

Then stop the group on the removed node with `Keel.stop_group(:default)` and wipe its data directory. A removed id can never join again. You get `:member_removed`. Give the replacement a new id.

To replace a dead member add the new one first. Remove the dead one after the new one votes. If you remove first you have fewer healthy voters during the change.

## Transfer leadership

Run it on the leader's node. Name a voter.

```elixir
Keel.transfer_leadership(:default, "b")
```

It returns `:ok` once the target leads. On any other node it returns `{:error, {:not_leader, leader_id}}`. If the target can't catch up it returns `{:error, :timeout}` after about five seconds. Do this before you stop a leader for maintenance. Restart members one at a time. Before the next one wait until `Keel.Cluster.status/2` shows equal `commit` and `applied`.

## When something breaks

### Quorum is lost

Writes fail with `:no_leader`, `:timeout` or `:unknown_outcome`. The status shows errors for a majority of members.

Bring the missing members back. Restart crashed nodes and fix the network. A member with its data directory comes back with its log. The group elects a leader as soon as a majority can talk. Call `Keel.start_group/2` again on any node whose group is gone.

Don't bootstrap the survivors as a new group. Don't copy log files between members. Don't put an old copy of a data directory back. A stale copy forgets votes and writes it acknowledged. That can lose acknowledged data.

Keel 0.1 has no tool to recover from the loss of a majority. It has none to export a snapshot either. If most disks are gone for good you can't rebuild the group in place.

### The disk is full

The log writer fails to write and stops. The log shows `log writer stopping after :enospc`. The group restarts its parts up to three times in a minute. If the disk is still full it stays down. `Keel.status/1` then exits.

Free space on the volume. Then call `Keel.start_group/2` with the same options. The member replays what was durable and catches up from the leader. Don't delete segment or snapshot files by hand.

To keep logs small lower `snapshot_threshold_bytes`. A snapshot lets the member drop old segments. Dropped segments move to `log/trash` and are freed a few megabytes at a time. So the space comes back over a few seconds. Files in `log/trash` are safe to delete by hand.

### A sync fails

A failed `datasync` is never retried. The kernel may already have dropped the dirty pages. The writer stops with the line `log writer stopping after` and the error. The group restarts from what is durable. No write was acknowledged on the strength of that sync.

If it was a one-off on a healthy disk the restart is enough. If it repeats the group gives up after three restarts in a minute. Check the disk with the host's tools. If it is failing replace the member on a new disk with a new id.

### A follower is slow

The group commits with the faster majority. One slow follower doesn't slow writes. But you lose your spare, because the slow one is now in every quorum.

Compare `last_index` and `applied` across `Keel.Cluster.status/2`. A follower that keeps falling behind gets a snapshot once the leader has compacted past it. Check that member's disk and CPU. The log shows `member b has N messages waiting` when its mailbox grows. If its disk is the cause replace it.

### A snapshot is stuck

A follower far behind gets a snapshot in chunks. The leader starts the transfer over in two cases. One is when the follower stays silent for `snapshot_ack_timeout_ticks` (100 ticks or 5 s) while chunks go out. The other is when the install doesn't finish within `snapshot_install_timeout_ticks` (600 ticks or 30 s) of the last chunk. A healthy transfer ends with the follower's `first_index` jumping forward.

If it never ends read the logs. On the leader a sender that stops with `snapshot_read_failed` has a bad snapshot file or disk. On the follower `install_failed` or `snapshot_dir_unusable` means its snapshot directory or disk is bad. A member that stops with `snapshot_unreadable` can't read a snapshot of its own. Fix the disk and restart the group on that member. If it still fails remove it. Add it back under a new id with an empty directory.

### Elections will not settle

Terms climb and leaders change often. Check that every member has the same timer options. Check that heartbeats get through the network. Check that no member is starved of CPU. Pre-vote and CheckQuorum are on by default. A member that can't reach the leader doesn't raise the term for everyone.
