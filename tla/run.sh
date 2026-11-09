#!/bin/sh
# usage: run.sh models | mutants | traces
#
# models  every config in models/ must pass
# mutants every config in mutants/ must fail on the property its EXPECT line names
# traces  simulator traces must be accepted and a corrupted one rejected

set -u

here=$(cd "$(dirname "$0")" && pwd)
cd "$here" || exit 1

failed=0

module_for() {
  case $(basename "$1" .cfg) in
    OldTermCommit | CommitOldTermByCount) echo OldTermCommit.tla ;;
    *) echo Raft.tla ;;
  esac
}

check_all() {
  for config in "$1"/*.cfg; do
    module=$(module_for "$config")
    "$here/check.sh" "$config" "$module" || failed=1
  done
}

case ${1:-models} in
  models) check_all models ;;
  mutants) check_all mutants ;;
  traces)
    work=$(mktemp -d -p "$here/.tlc" 2>/dev/null || mktemp -d)
    root=$(cd "$here/.." && pwd)
    for seed in ${TRACE_SEEDS:-1 2 3 4 5}; do
      "$here/trace.sh" "$root" "$work" "$seed" || failed=1
    done
    rm -f "$work"/*.ndjson "$work"/*.cfg
    rmdir "$work" 2>/dev/null
    ;;
  *)
    echo "usage: run.sh models | mutants | traces"
    exit 2
    ;;
esac

exit "$failed"
