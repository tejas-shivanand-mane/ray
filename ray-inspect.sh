#!/bin/bash
SCRIPT=${1:-"test_recovery.py"}
PATTERN=${2:-"PASS|FAIL|owner|Owner|OWNER|fail|Fail|recover|Recover|object|gossip|Gossip|GOSSIP|ERROR|WARN|Step|killed|dead|Dead|SUCCESS|EXECUTING|lineage"}
EXCLUDE="cannot be loaded|pip install|dashboard|aiohttp|FutureWarning|RLIMIT|gc_collect|Automatically increasing|Setting node|Starting node|Process STDOUT|Determine to start|Starting Python|Stopping Python|logging.cc|SIGTERM handler|client_builder|UserWarning|warnings.warn|state-dump|deadline_timer|CheckDeadSubscribers|flush_task_events|failed to report|failed requests|sent failure"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTDIR="$HOME/ray-logs/$TIMESTAMP"
mkdir -p "$OUTDIR"

echo "================================================"
echo "Ray Debug Inspector"
echo "Script:  $SCRIPT"
echo "Output:  $OUTDIR"
echo "================================================"

export RAY_DEDUP_LOGS=0
export RAY_BACKEND_LOG_LEVEL=info
export PYTHONUNBUFFERED=1

# Run test via wrapper that handles Docker container killing
if docker ps 2>/dev/null | grep -q "ray-head"; then
    echo "Running via Docker wrapper..."
    ~/ray-run-test.sh "$SCRIPT" 2>&1 \
        | grep -Ev "$EXCLUDE" \
        | tee "$OUTDIR/test_output.log"
else
    echo "Running locally..."
    cd ~/work/ray
    conda run -n ray-dev \
        python "$HOME/work/ray/$SCRIPT" 2>&1 \
        | grep -Ev "$EXCLUDE" \
        | tee "$OUTDIR/test_output.log"
fi

# Collect Docker logs
if docker ps 2>/dev/null | grep -q "ray-head"; then
    echo ""
    echo "Collecting Docker logs..."

    HEAD_SESSION=$(docker exec ray-head bash -c \
        "ls -td /tmp/ray/session_* 2>/dev/null | grep -v session_latest | head -1")
    WA_SESSION=$(docker exec ray-worker-a bash -c \
        "ls -td /tmp/ray/session_* 2>/dev/null | grep -v session_latest | head -1" 2>/dev/null)
    WB_SESSION=$(docker exec ray-worker-b bash -c \
        "ls -td /tmp/ray/session_* 2>/dev/null | grep -v session_latest | head -1")

    echo "Head session:     $HEAD_SESSION"
    echo "Worker-a session: $WA_SESSION"
    echo "Worker-b session: $WB_SESSION"

    mkdir -p "$OUTDIR/head" \
             "$OUTDIR/worker_a" \
             "$OUTDIR/worker_b"

    [ -n "$HEAD_SESSION" ] && \
        docker cp "ray-head:$HEAD_SESSION/logs/." \
            "$OUTDIR/head/" 2>/dev/null

    [ -n "$WA_SESSION" ] && \
        docker cp "ray-worker-a:$WA_SESSION/logs/." \
            "$OUTDIR/worker_a/" 2>/dev/null

    [ -n "$WB_SESSION" ] && \
        docker cp "ray-worker-b:$WB_SESSION/logs/." \
            "$OUTDIR/worker_b/" 2>/dev/null

    grep -E "$PATTERN" "$OUTDIR/head/gcs_server.out" \
        2>/dev/null | grep -Ev "$EXCLUDE" \
        > "$OUTDIR/gcs_filtered.log"

    cat "$OUTDIR/head/python-core-worker-"*.log \
        "$OUTDIR/worker_a/python-core-worker-"*.log \
        "$OUTDIR/worker_b/python-core-worker-"*.log \
        2>/dev/null > "$OUTDIR/core_worker_full.log"

    grep -E "$PATTERN" "$OUTDIR/core_worker_full.log" \
        | grep -Ev "$EXCLUDE" \
        > "$OUTDIR/core_worker_filtered.log"

    cat "$OUTDIR/head/"*.log \
        "$OUTDIR/head/"*.out \
        "$OUTDIR/worker_a/"*.log \
        "$OUTDIR/worker_a/"*.out \
        "$OUTDIR/worker_b/"*.log \
        "$OUTDIR/worker_b/"*.out \
        2>/dev/null | sort \
        | grep -Ev "$EXCLUDE" \
        > "$OUTDIR/all_logs_sorted.log"

    grep -E "$PATTERN" "$OUTDIR/all_logs_sorted.log" \
        > "$OUTDIR/all_logs_filtered.log"

    grep "GOSSIP_" "$OUTDIR/all_logs_sorted.log" \
        > "$OUTDIR/gossip_logs.log"

    echo "Logs saved to $OUTDIR"
fi

ln -sfn "$OUTDIR" "$HOME/ray-logs/latest"

echo ""
echo "================================================"
echo "DONE — $OUTDIR"
echo "================================================"
grep -E "✓|✗|PASS|FAIL" "$OUTDIR/test_output.log" | tail -30
echo "================================================"
echo "Quick commands:"
echo "  cat ~/ray-logs/latest/test_output.log"
echo "  cat ~/ray-logs/latest/gcs_filtered.log"
echo "  cat ~/ray-logs/latest/core_worker_filtered.log"
echo "  cat ~/ray-logs/latest/gossip_logs.log"
echo "  grep 'GOSSIP_' ~/ray-logs/latest/core_worker_full.log"
echo "================================================"
