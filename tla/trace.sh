#!/bin/sh
# usage: trace.sh ROOT WORKDIR SEED
#
# Runs one short simulator seed with a trace and has TLC check the trace.
# Seed 1 also checks that a corrupted copy is rejected.

set -u

root=$1
work=$2
seed=$3
here=$(cd "$(dirname "$0")" && pwd)
trace="$work/trace-$seed.ndjson"

(cd "$root" && mix keel.sim --seed "$seed" --steps "${TRACE_STEPS:-200}" --trace "$trace" > /dev/null) || exit 1

prevote=$(sed -n '1s/.*"pre_vote":\(true\|false\).*/\1/p' "$trace" | tr a-z A-Z)

make_config() {
  sed -e "s|TRACEFILE|$1|" -e "s|PREVOTE|$prevote|" -e "s|EXPECTLINE|$2|" "$here/TraceCheck.cfg.template" > "$3"
}

config="$work/trace-$seed.cfg"
make_config "$trace" "Done" "$config"
lines=$(($(wc -l < "$trace") - 1))
echo "seed $seed: $lines trace lines, pre_vote $prevote"
TLC_WORKERS=1 "$here/check.sh" "$config" TraceCheck.tla || exit 1

if [ "$seed" = "1" ]; then
  bad="$work/trace-bad.ndjson"
  # The first persist claims a durable length no log has
  awk '/"ev":"persisted"/ && !done { sub(/"durable":[0-9]+/, "\"durable\":99"); done = 1 } { print }' "$trace" > "$bad"
  make_config "$bad" "Rejected" "$work/trace-bad.cfg"
  echo "seed $seed corrupted: first persist line says durable 99"
  TLC_WORKERS=1 "$here/check.sh" "$work/trace-bad.cfg" TraceCheck.tla || exit 1
fi
