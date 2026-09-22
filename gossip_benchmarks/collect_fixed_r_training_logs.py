"""Collect existing native logs for the failed training case; never start Ray.

Match the saved report's unique node/cluster IDs or GCS process IDs, rather than
session_latest (which points at a later, passing backpressure case). Bound both
the scan and captured content. Missing/rotated logs are reported explicitly.
"""

import argparse
import json
import os
from pathlib import Path
import re


def excerpt(path, head_bytes, tail_bytes):
    with path.open("rb") as stream:
        size = os.fstat(stream.fileno()).st_size
        head = stream.read(min(head_bytes, size))
        tail_start = max(len(head), size - tail_bytes)
        stream.seek(tail_start)
        tail = stream.read(tail_bytes)
    gap = tail_start - len(head)
    marker = f"\n... {gap} bytes omitted ...\n".encode() if gap else b""
    return {"path": str(path), "size_bytes": size, "omitted_bytes": gap,
            "text": (head + marker + tail).decode("utf-8", errors="replace")}


def main():
    root = Path(os.environ.get("RAY_RECOVERY_OUTPUT_DIR", str(Path.home() / "ray-coverage")))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=root / "coverage-retry.json")
    parser.add_argument("--temp-dir", type=Path)
    parser.add_argument("--output", type=Path, default=root / "training-prefetch-native-logs.json")
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    matches = [(key, value) for key, value in report["cases"].items()
               if key.startswith("training_prefetch/")
               and value.get("validation_status") == "failed"]
    if len(matches) != 1:
        parser.error("Expected exactly one failed training-prefetch case in the report")
    key, case = matches[0]
    observation = case.get("observation_before_shutdown", {})
    identifiers = [case.get("cluster_id"), case.get("original_head_node_id"),
                   case.get("replacement_head_node_id"),
                   observation.get("latest", {}).get("coordinator_node_id")]
    identifiers = [value.encode() for value in identifiers if value]
    pids = [case.get("original_gcs_pid"), case.get("replacement_gcs_pid")]
    pid_patterns = [re.compile(rb"\b[DIWEF]\s+" + str(pid).encode() + rb"\s+")
                    for pid in pids if pid]
    roots = [args.temp_dir] if args.temp_dir else [
        Path(value) for value in (
            os.environ.get("RAY_RECOVERY_TEMP_DIR"), os.environ.get("RAY_TMPDIR"),
            str(Path.home() / "raytmp"), "/tmp/ray",
        ) if value
    ]
    # Ray appends /ray to RAY_TMPDIR; --temp-dir may instead name that
    # already-expanded directory. Cover both without a recursive disk scan.
    roots = list(dict.fromkeys(candidate.expanduser().resolve()
                               for path in roots for candidate in (path, path / "ray")))
    result = {"source_report": str(args.report.resolve()), "case": key,
              "benchmark_started": False, "searched_roots": [str(p) for p in roots],
              "matched_sessions": [], "files": [], "read_errors": [],
              "capture_limit_bytes": 8 * 1024**2, "capture_limit_reached": False}
    sessions = set()
    for directory in roots:
        if not directory.is_dir():
            continue
        for session in directory.glob("session_*"):
            if session.is_symlink() or not session.is_dir():
                continue
            logs = session / "logs"
            candidates = sorted(set(logs.glob("gcs_server*.out")) |
                                set(logs.glob("raylet*.out")))
            for path in candidates:
                try:
                    with path.open("rb") as stream:
                        prefix = stream.read(64 * 1024)
                    if (any(value in prefix for value in identifiers)
                            or (path.name.startswith("gcs_server")
                                and any(pattern.search(prefix) for pattern in pid_patterns))):
                        sessions.add(session)
                        break
                except OSError as exc:
                    result["read_errors"].append({"path": str(path), "error": str(exc)})
    result["matched_sessions"] = [str(path) for path in sorted(sessions)]
    used = 0
    # Capture raylet scheduling/pull state first, then core-worker replay logs.
    for patterns, head_bytes, tail_bytes in (
        (("debug_state*.txt", "raylet*.out", "raylet*.err"), 16 * 1024, 256 * 1024),
        (("python-core-worker-*.log", "python-core-driver-*.log"), 4096, 64 * 1024),
    ):
        paths = sorted({path for session in sessions for pattern in patterns
                        for path in (session / "logs").glob(pattern)})
        for path in paths:
            if used + head_bytes + tail_bytes > result["capture_limit_bytes"]:
                result["capture_limit_reached"] = True
                break
            try:
                item = excerpt(path, head_bytes, tail_bytes)
                used += len(item["text"].encode())
                result["files"].append(item)
            except OSError as exc:
                result["read_errors"].append({"path": str(path), "error": str(exc)})
    result["status"] = "collected" if result["files"] else "logs_missing"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(result, indent=2))
    temporary.replace(args.output)
    print(f"{result['status']}: {len(result['files'])} existing log files")
    print(f"Upload: {args.output}")
    if not result["files"]:
        print("No matching logs found. Use --temp-dir if Ray files were stored elsewhere.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
