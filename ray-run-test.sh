#!/bin/bash
SCRIPT=${1:-"test_owner_died_multinode.py"}

docker exec ray-head rm -f /tmp/ready_to_kill.txt 2>/dev/null
docker cp ~/work/ray/$SCRIPT ray-head:/tmp/$SCRIPT

docker exec \
    -e RAY_DEDUP_LOGS=0 \
    -e RAY_BACKEND_LOG_LEVEL=info \
    ray-head \
    python /tmp/$SCRIPT &
TEST_PID=$!

# Wait for signal (only needed for tests that use file signal)
for i in $(seq 1 120); do
    if docker exec ray-head \
        test -f /tmp/ready_to_kill.txt 2>/dev/null; then
        echo "Signal received — killing ray-worker-a..."
        docker stop ray-worker-a
        echo "ray-worker-a killed"
        break
    fi
    sleep 1
done

wait $TEST_PID
echo "Test done"
