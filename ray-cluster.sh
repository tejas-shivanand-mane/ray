#!/bin/bash
# Start a 3-node Ray cluster on localhost
# Head + Worker A + Worker B

PORT=6399
ACTION=${1:-"start"}

stop_cluster() {
    echo "Stopping Ray cluster..."
    ray stop --force 2>/dev/null
    sleep 2
    pkill -f "ray::" 2>/dev/null
    pkill -f "raylet" 2>/dev/null
    pkill -f "gcs_server" 2>/dev/null
    pkill -f "plasma_store" 2>/dev/null
    sleep 2
    echo "Cluster stopped"
}

start_cluster() {
    # Stop any existing cluster first
    stop_cluster
    sleep 2

    echo "================================================"
    echo "STEP 1: Start HEAD node"
    echo "================================================"
    ray start --head \
        --port=$PORT \
        --num-cpus=4 \
        --resources='{"head":1}' \
        --min-worker-port=11000 \
        --max-worker-port=11100 \
        --object-store-memory=2000000000
    sleep 3

    echo "================================================"
    echo "STEP 2: Start WORKER A"
    echo "================================================"
    ray start \
        --address=127.0.0.1:$PORT \
        --num-cpus=4 \
        --resources='{"worker_a":1}' \
        --min-worker-port=11101 \
        --max-worker-port=11200 \
        --object-store-memory=2000000000
    sleep 3

    echo "================================================"
    echo "STEP 3: Start WORKER B"
    echo "================================================"
    ray start \
        --address=127.0.0.1:$PORT \
        --num-cpus=4 \
        --resources='{"worker_b":1}' \
        --min-worker-port=11201 \
        --max-worker-port=11300 \
        --object-store-memory=2000000000
    sleep 3

    echo "================================================"
    echo "Cluster status:"
    ray status
    echo "================================================"
    echo "Connect with: ray.init(address='ray://127.0.0.1:$PORT')"
    echo "Or:           ray.init(address='auto')"
    echo "================================================"
}

status_cluster() {
    ray status
}

case $ACTION in
    start)  start_cluster ;;
    stop)   stop_cluster ;;
    status) status_cluster ;;
    restart)
        stop_cluster
        sleep 3
        start_cluster
        ;;
    *)
        echo "Usage: $0 [start|stop|status|restart]"
        exit 1
        ;;
esac
