#!/bin/sh
# Runs TLC on one module and config and checks the outcome.
#
# usage: check.sh CONFIG MODULE
#
# A config with no EXPECT line must pass. A config with a line
# "\* EXPECT: Name" must fail on the invariant or property called Name.
# A config with the line "\* EXPECT: Done" holds a trace. It passes when TLC
# reaches the end of the trace.

set -u

config=$1
module=$2
jar=${TLA_JAR:-$HOME/.local/share/tla/tla2tools.jar}
java=${JAVA:-java}
workers=${TLC_WORKERS:-auto}
expect=$(sed -n 's/^\\\* EXPECT: *//p' "$config")
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$here/.tlc"
meta=$(mktemp -d -p "$here/.tlc")
out=$(mktemp)

start=$(date +%s)
limit=${TLC_TIMEOUT:-0}
timeout "$limit" $java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -cleanup -deadlock \
  -workers "$workers" -metadir "$meta" -config "$config" "$module" > "$out" 2>&1
code=$?
seconds=$(( $(date +%s) - start ))

generated=$(sed -n 's/^\([0-9,]*\) states generated, \([0-9,]*\) distinct states found.*/\1/p' "$out" | tail -1)
distinct=$(sed -n 's/^\([0-9,]*\) states generated, \([0-9,]*\) distinct states found.*/\2/p' "$out" | tail -1)
depth=$(sed -n 's/^The depth of the complete state graph search is \([0-9]*\).*/\1/p' "$out" | tail -1)
violated=$(sed -n 's/^Error: \(Invariant\|Action property\|Temporal properties were\) \([A-Za-z]*\).*violated.*/\2/p' "$out" | head -1)
stats="$distinct distinct states, $generated generated, depth ${depth:-?}, ${seconds}s"

report() {
  echo "$(basename "$config") $1: $stats"
}

finish() {
  rmdir "$meta" 2>/dev/null
  rm -f "$out" "$here"/*_TTrace_*.bin "$here"/*_TTrace_*.tla
  exit "$1"
}

if [ -z "$expect" ]; then
  if [ "$code" -eq 0 ]; then
    report "ok"
    finish 0
  fi

  report "FAILED"
  tail -40 "$out"
  finish 1
fi

if [ "$expect" = "Rejected" ]; then
  if [ "$code" -eq 0 ] && [ -z "$violated" ]; then
    report "rejected as it must"
    finish 0
  fi

  report "ACCEPTED a corrupted trace"
  finish 1
fi

if [ "$expect" = "Done" ]; then
  if [ "$violated" = "NotDone" ]; then
    report "accepted"
    finish 0
  fi

  report "REJECTED"
  finish 1
fi

if [ "$violated" = "$expect" ]; then
  report "fails on $violated as it must"
  finish 0
fi

report "FAILED, expected a violation of $expect and got ${violated:-none}"
tail -40 "$out"
finish 1
