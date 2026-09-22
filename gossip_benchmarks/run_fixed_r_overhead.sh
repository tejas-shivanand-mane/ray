#!/usr/bin/env bash
# Existing ray-dev source build; no build, install or failure injection.
set -euo pipefail
cd "$(dirname "$0")/.."
result_root="${RAY_RECOVERY_OUTPUT_DIR:-$HOME/ray-coverage}"
mkdir -p "$result_root"
export TMPDIR="${RAY_RECOVERY_TEMP_DIR:-$HOME/raytmp}"
mkdir -p "$TMPDIR"
for directory in "$result_root" "$TMPDIR"; do
  case "$(stat -f -c %T "$directory")" in
    tmpfs|ramfs) echo "Use disk-backed result and temporary directories: $directory" >&2; exit 2 ;;
  esac
done
export RAY_TMPDIR="$TMPDIR"
result_dir="$(mktemp -d "$result_root/overhead.XXXXXX")"
exec python gossip_benchmarks/run_fixed_r_overhead.py \
  --result-directory "$result_dir" --output "$result_root/overhead.json" "$@"
