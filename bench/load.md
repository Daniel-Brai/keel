# Load results

Run on 2026-11-17 with `make bench.load`. Three members on this host with 1 KiB values.

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

## Open loop

Writes start on a fixed schedule whether or not earlier ones finished. Latency runs from the planned start. So a backlog shows up as latency. A write that would be the 4097th still running is shed.

| Offered/s | Achieved/s | p50 ms | p99 ms | max ms | Errors | Shed |
|---|---|---|---|---|---|---|
| 500 | 499 | 4.54 | 46.94 | 63.70 | 0 | 0 |
| 1000 | 1000 | 3.59 | 29.40 | 95.23 | 0 | 0 |
| 2000 | 1999 | 6.17 | 70.14 | 91.76 | 0 | 0 |
| 3000 | 3000 | 6.96 | 92.78 | 177.09 | 0 | 0 |
| 4000 | 3998 | 7.50 | 55.35 | 85.33 | 0 | 0 |
| 5000 | 4995 | 11.37 | 130.29 | 158.06 | 0 | 0 |

## Batching sweep

Closed loop. Each row is a fresh cluster. Two seconds of warm-up are dropped.

| Setting | Writes/s | p50 ms | p99 ms | Errors |
|---|---|---|---|---|
| max_append_entries 16, max_inflight_appends 1, 64 clients | 4542 | 12.04 | 48.04 | 0 |
| max_append_entries 16, max_inflight_appends 8, 64 clients | 4704 | 11.43 | 43.34 | 0 |
| max_append_entries 16, max_inflight_appends 32, 64 clients | 5207 | 11.54 | 30.49 | 0 |
| max_append_entries 64, max_inflight_appends 1, 64 clients | 4851 | 11.69 | 35.64 | 0 |
| max_append_entries 64, max_inflight_appends 8, 64 clients | 3369 | 16.48 | 53.84 | 0 |
| max_append_entries 64, max_inflight_appends 32, 64 clients | 2641 | 20.49 | 67.91 | 0 |
| max_append_entries 256, max_inflight_appends 1, 64 clients | 3435 | 16.73 | 47.48 | 0 |
| max_append_entries 256, max_inflight_appends 8, 64 clients | 3283 | 16.78 | 57.65 | 0 |
| max_append_entries 256, max_inflight_appends 32, 64 clients | 4437 | 13.61 | 32.83 | 0 |
| defaults, 8 clients | 1391 | 3.69 | 16.63 | 0 |
| defaults, 32 clients | 3038 | 9.56 | 31.76 | 0 |
| defaults, 128 clients | 5292 | 20.18 | 78.14 | 0 |

## Soak

One cluster at a fixed open-loop rate on 10000 keys. So the stored state stays the same size and only the log grows. Memory is each member's BEAM total. Disk is all three data directories.

| Minute | Achieved/s | p50 ms | p99 ms | max ms | Errors | Memory MiB | Disk MiB |
|---|---|---|---|---|---|---|---|
| 1 | 1501 | 6.33 | 152.10 | 247.19 | 0 | 162 / 162 / 157 | 384 |
| 2 | 1501 | 5.98 | 73.97 | 328.33 | 0 | 259 / 263 / 258 | 768 |
| 3 | 1501 | 6.98 | 187.98 | 441.55 | 0 | 104 / 102 / 97 | 414 |
| 4 | 1501 | 7.20 | 152.00 | 463.16 | 0 | 199 / 202 / 198 | 798 |
| 5 | 1501 | 10.59 | 129.46 | 388.23 | 0 | 305 / 299 / 299 | 990 |
| 6 | 1501 | 9.27 | 143.97 | 296.03 | 0 | 129 / 127 / 126 | 414 |
| 7 | 1501 | 8.52 | 132.61 | 354.26 | 0 | 230 / 228 / 227 | 606 |
| 8 | 1501 | 5.85 | 118.57 | 343.05 | 0 | 326 / 327 / 329 | 990 |
| 9 | 1501 | 6.36 | 168.44 | 514.96 | 0 | 151 / 154 / 153 | 414 |
| 10 | 1501 | 6.01 | 61.27 | 175.78 | 0 | 251 / 252 / 254 | 798 |
| 11 | 1501 | 6.11 | 127.55 | 438.90 | 0 | 79 / 85 / 78 | 222 |
| 12 | 1501 | 6.20 | 107.14 | 278.86 | 0 | 182 / 184 / 181 | 606 |
| 13 | 1501 | 6.30 | 104.69 | 308.57 | 0 | 281 / 283 / 281 | 798 |
| 14 | 1501 | 5.87 | 211.88 | 502.72 | 0 | 107 / 110 / 109 | 414 |
| 15 | 1501 | 5.65 | 53.76 | 159.42 | 0 | 213 / 208 / 214 | 606 |
