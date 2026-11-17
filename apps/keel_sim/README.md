# keel_sim

Runs whole Keel clusters in one process from a seed. It never ships in a release.

The simulator drives the real Raft core, log store and state machines. Only the disk, the network, time and the clients are models. Each step it picks one action. It might deliver a message, drop one, crash a member or split the network. After every step it checks that no safety rule broke.

The same seed always gives the same run. A failure replays exactly and shrinks to a short schedule.

Run one cluster or a sweep of seeds.

```sh
mix keel.sim --seed 42
mix keel.sim --seed 1 --seeds 300
```

Replay a saved run and shrink it to the fewest steps that still fail.

```sh
mix keel.sim --replay run.term --shrink
```
