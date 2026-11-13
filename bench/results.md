# Benchmark results

Run on 2026-11-17 with `make bench`. Every number below comes from this machine.

## Machine

| Item | Recorded |
|---|---|
| CPU | 11th Gen Intel(R) Core(TM) i5-1145G7 @ 2.60GHz, 8 logical cores, governor performance |
| Memory | 23.2 GiB |
| Disk | WDC PC SN730 SDBPNTY-512G-1006 476.9G nvme |
| Disk power-loss protection | not known |
| Filesystem | btrfs on /dev/nvme0n1p6[/home], options rw,relatime,seclabel,compress=zstd:1,ssd,discard=async,space_cache=v2,subvolid=257,subvol=/home |
| OS | Linux 7.3.3-200.fc44.x86_64 |
| BEAM | OTP 29, Elixir 1.20.5, 8 schedulers, +sbwt at its default |
| Keel | no commit yet. Working tree of 2026-11-17 |

## Against the targets

| Target | Measured | Needed | Verdict |
|---|---|---|---|
| Failover | p50 616.94 ms, p99 890.89 ms, max 899.79 ms over 100 kills | p99 under 1500 ms | met |
| Write latency, one client | p50 1.90 ms, p99 7.98 ms | p50 under 3 ms, p99 under 15 ms | met |
| Write throughput, 64 clients | 3764 writes/s, 0 errors | at least 10000 writes/s | missed |
| ReadIndex read latency | p50 0.14 ms, p99 1.00 ms | p99 under 5 ms | met |
| Restart | 2.6 s to recover 320 MiB, 8.4 s scaled to 1 GiB | ready to vote within 10 s at 1 GiB of log (scaled and without a snapshot) | met |

## Method

Three members on this host. Each member is a `:peer` BEAM node with its own data directory on the disk above. So the three logs share one device. Distribution runs over loopback. Timers are the defaults. Ticks are 50 ms. The election timeout is 10 to 20 ticks. The heartbeat is every 2 ticks. Pre-vote is on.

- Clients call the `Keel.KV` facade from the benchmark node. One writer process on that node serves every client. So 64 clients share one process.
- One writer process holds one session. A session lets 16 commands wait at once by default. With 64 clients most writes then fail with `:too_many_pending`. So the run first raises `session_max_pending` to 128 with `set_limits`.
- After the 64 client run the slowest follower needed 22 ms to hold everything the leader held.
- Values are 1 KiB of random bytes. The file system can't compress them.
- Write latency: one client, closed loop, 500 warm-up writes dropped, 5000 measured.
- Throughput: 64 clients, closed loop, 3 s of warm-up dropped, 15 s measured.
- Reads: `Keel.KV.get` with the default read index mode, one client, 5000 measured.
- Failover: the leader is killed with SIGKILL. A write starts every 10 ms until one returns `:ok`. The time runs from the kill to that reply. Then the killed member restarts and the cluster settles before the next kill.
- Restart: a follower holding about 320 MiB of segment files and no snapshot is killed and started again. The time is the call that opens its log and starts the group. It leaves out the 239 ms to boot the VM. The member then needed 3805 ms to bring its state machine level with the leader. The 1 GiB figure is the measured time scaled by size. A log with a 256 MiB snapshot on top was not measured.
- Segment files are preallocated. Their size is an upper bound on the log bytes. The log held 253564 entries.

## Raw numbers

| Run | Samples | p50 ms | p90 ms | p99 ms | max ms |
|---|---|---|---|---|---|
| 4 KiB write plus datasync | 2000 | 0.73 | 0.82 | 0.99 | 16.50 |
| Write, one client | 5000 | 1.90 | 2.61 | 7.98 | 50.52 |
| Write, 64 clients | 56471 | 11.54 | 37.63 | 58.06 | 86.02 |
| Read, one client | 5000 | 0.14 | 0.22 | 1.00 | 2.24 |
| Read, 16 writers running | 3000 | 3.41 | 7.26 | 14.96 | 55.87 |
| Failover | 100 | 616.94 | 828.24 | 890.89 | 899.79 |

## Notes and gaps

- The read target is for a quiet cluster. Reads with 16 writers running are in the raw numbers.
- The disk's power-loss protection is not known. One 4 KiB write with a datasync took about the p50 in the first row of the raw numbers. Every write pays at least that.
- The 64 clients share one writer process on the benchmark node. The three logs share one disk.
- Not measured: three separate hosts, other stores, a long soak run, open-loop load and a batching sweep.
