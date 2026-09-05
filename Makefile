.DEFAULT_GOAL := help

.PHONY: help setup format lint purity test cluster sweep sim tlc trace lazyfs power bench bench.load release.check check

# Lists every target with the comment above it
help:
	@awk '/^# / { doc = substr($$0, 3); next } /^[a-z][a-z.-]*:/ { name = $$1; sub(/:.*/, "", name); if (doc != "") printf "  %-8s %s\n", name, doc; doc = "" }' $(MAKEFILE_LIST)

# Fetches dependencies and installs the git hooks
setup:
	mix setup

# Formats every app and the TLA+ models
format:
	mix format
	for f in tla/*.tla; do tlafmt -i "$$f"; done

# Runs Credo strict, including the purity check
lint:
	mix credo --strict

# Fails if a pure module calls impure code in another module
purity:
	mix keel.check_purity

# Runs every app's tests
test:
	mix test

# Runs the real cluster tests that start peer nodes
cluster:
	cd apps/keel_kv && mix test --only cluster

# Runs the heavy simulator sweeps that mix test skips
sweep:
	cd apps/keel_sim && mix test --only sweep

# Runs one simulated cluster, make sim SEED=42
sim:
	mix keel.sim --seed $(or $(SEED),1)

# Model checks the TLA+ models. Base configs must pass and mutants must fail
tlc:
	for f in tla/*.tla; do tlafmt -c "$$f" > /dev/null || exit 1; done
	tla/run.sh models
	tla/run.sh mutants

# Has TLC check short simulator traces against the model and a corrupted one
trace:
	tla/run.sh traces

# Runs the LazyFS power loss tests. They skip when LazyFS is not installed
lazyfs:
	cd apps/keel_kv && mix test --only lazyfs

# Runs the dm-log-writes power loss tests. They need root and skip without it
power:
	cd apps/keel_kv && mix test --only power

# Measures latency and throughput on a real three member cluster and writes bench/results.md
bench:
	cd apps/keel_kv && MIX_ENV=prod mix run ../../bench/bench.exs

# Runs open loop load, a batching sweep and a soak run and writes bench/load.md
bench.load:
	cd apps/keel_kv && MIX_ENV=prod mix run ../../bench/load.exs

# Builds the release and fails if the simulator is inside it
release.check:
	MIX_ENV=prod mix release keel --overwrite --quiet
	@! ls _build/prod/rel/keel/lib | grep -q keel_sim || (echo "keel_sim is in the release" && exit 1)
	@echo "Release holds no simulator"

# Runs everything CI runs on a pull request
check:
	mix format --check-formatted
	mix compile --warnings-as-errors --force
	mix credo --strict
	mix keel.check_purity
	mix test
	mix dialyzer
	$(MAKE) release.check
