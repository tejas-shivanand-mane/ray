#!/usr/bin/env bash
# Existing ray-dev source build; default one bounded training-only OFF/ON pair.
set -euo pipefail
cd "$(dirname "$0")/.."
result_root="${RAY_RECOVERY_OUTPUT_DIR:-$HOME/ray-coverage}"
export TMPDIR="${RAY_RECOVERY_TEMP_DIR:-$HOME/raytmp}"
mkdir -p "$result_root" "$TMPDIR"
for directory in "$result_root" "$TMPDIR"; do
  case "$(stat -f -c %T "$directory")" in
    tmpfs|ramfs) echo "Use disk-backed result and temporary directories." >&2; exit 2 ;;
  esac
done
export RAY_TMPDIR="$TMPDIR"
result_dir="$(mktemp -d "$result_root/training-comparison.XXXXXX")"
exec python gossip_benchmarks/run_fixed_r_train_comparison.py \
  --result-directory "$result_dir" --output "$result_root/training-comparison.json" "$@"
