# Keel

A replicated key-value store on Raft.

Keel is a Raft library and a linearizable key-value store built on it. A cluster is three, five or seven BEAM nodes. Each node runs one member with a durable log on its own disk.

The consensus core is a pure function. It takes a state and an event and returns a new state and a list of effects. A simulator runs whole clusters in one process from a seed. A failing run replays exactly.

## Download

Each [release](https://github.com/Daniel-Brai/keel/releases) has a tarball for Linux and macOS. It needs nothing else installed.

Download a tarball and its checksum from the releases page in a browser or with `curl`. Set `VERSION` to the release you want.

```sh
VERSION=v0.2.0
BASE=https://github.com/Daniel-Brai/keel/releases/download/$VERSION
curl -LO "$BASE/keel-$VERSION-linux-x86_64.tar.gz"
curl -LO "$BASE/keel-$VERSION-linux-x86_64.tar.gz.sha256"
```

With the GitHub CLI `gh release download --repo Daniel-Brai/keel --pattern 'keel-*-linux-x86_64.tar.gz*'` fetches the newest one.

Check the file and unpack it.

```sh
sha256sum -c "keel-$VERSION-linux-x86_64.tar.gz.sha256"
tar -xzf "keel-$VERSION-linux-x86_64.tar.gz"
```

Start a member. Every member gets the same `KEEL_MEMBERS` and `KEEL_CLUSTER_ID` and its own `KEEL_MEMBER_ID` and node name. Make the cluster id once with `openssl rand -hex 16`. Use `KEEL_START=bootstrap` on the first start of a new cluster.

```sh
RELEASE_DISTRIBUTION=name RELEASE_NODE=a@10.0.0.1 RELEASE_COOKIE=secret \
  KEEL_MEMBER_ID=a KEEL_START=bootstrap \
  KEEL_MEMBERS=a=a@10.0.0.1,b=b@10.0.0.2,c=c@10.0.0.3 \
  KEEL_CLUSTER_ID=9f1c0e7a5b3d42e8a6c4b2d0e8f61a37 \
  keel/bin/keel daemon
```

On macOS swap `linux-x86_64` for `macos-arm64` and use `shasum -a 256 -c` in place of `sha256sum -c`. To build a tarball yourself run `MIX_ENV=prod mix release keel`. `docs/operations.md` covers every variable and how to grow or shrink a cluster.

## Apps

- `keel_raft` is the Raft library. It keeps the log in the same order on every member and hands committed entries to a state machine you supply.
- `keel_kv` is the key-value store built on it. It has compare and set, scans and safe retries.
- `keel_sim` runs whole clusters in one process from a seed and checks every step. It never ships in a release.

## Working on it

Install the versions in `.tool-versions`. Then run the setup.

```sh
make setup
```

Run every check CI runs before you push.

```sh
make check
```

Run one simulated cluster from a seed. The same seed always prints the same trace hash.

```sh
make sim SEED=42
```

`mix keel.sim --seed 3 --steps 200 --trace run.ndjson` writes the steps of one short run as lines of JSON. `make trace` does this and checks the lines against the model.

## Other targets

`make help` lists them all. These are the ones beyond `make check`.

- `make cluster` runs the tests that start real peer nodes.
- `make sweep` runs the heavy simulator sweeps.
- `make tlc` checks the TLA+ models in `tla/` with TLC. Every config in `tla/models` must pass. Every config in `tla/mutants` breaks one rule and must fail. It needs Java and the TLA+ tools jar. Set `TLA_JAR` if the jar is not at `~/.local/share/tla/tla2tools.jar`. It first checks that every model is formatted with `tlafmt`. `make format` formats them.
- `make trace` runs short simulator traces and has TLC check each one against the model. It also checks that a corrupted trace is rejected.
- `make bench` starts three real members on this machine and writes `bench/results.md`. It takes about fifteen minutes and needs a few GiB of free disk.
- `make lazyfs` and `make power` run the power loss tests. They skip with a message when LazyFS or root is missing.

