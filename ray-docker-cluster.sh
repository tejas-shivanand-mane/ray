#!/bin/bash
ACTION=${1:-"start"}
COMPOSE_FILE="$HOME/work/ray/docker-compose-cluster.yml"

stop_cluster() {
    echo "Stopping Docker Ray Cluster..."
    docker compose -f $COMPOSE_FILE down 2>/dev/null
    sleep 2
    echo "Cluster stopped"
}

start_cluster() {
    stop_cluster
    sleep 2

    echo "================================================"
    echo "Starting Docker Ray Cluster"
    echo "================================================"

    docker compose -f $COMPOSE_FILE up -d

    echo "Waiting for cluster to initialize..."
    sleep 25

    echo ""
    echo "Container status:"
    docker ps | grep ray

    echo ""
    echo "Head node logs:"
    docker logs ray-head --tail=10

    echo ""
    echo "================================================"
    echo "Cluster ready"
    echo "Head IP: 172.20.0.10:6399"
    echo "Connect: ray.init(address='ray://127.0.0.1:10001')"
    echo "================================================"
}

status_cluster() {
    echo "Container status:"
    docker ps | grep ray
    echo ""
    echo "Head logs (last 5):"
    docker logs ray-head --tail=5 2>/dev/null
}

restart_cluster() {
    stop_cluster
    sleep 3
    start_cluster
}

kill_node() {
    NODE=${1:-"ray-worker-a"}
    echo "Killing node: $NODE"
    docker stop $NODE
    echo "$NODE stopped — objects on this node are gone"
}

revive_node() {
    NODE=${1:-"ray-worker-a"}
    echo "Reviving node: $NODE"
    docker start $NODE
    sleep 8
    echo "$NODE started"
}

case $ACTION in
    start)   start_cluster ;;
    stop)    stop_cluster ;;
    status)  status_cluster ;;
    restart) restart_cluster ;;
    kill)    kill_node $2 ;;
    revive)  revive_node $2 ;;
    *)
        echo "Usage: $0 [start|stop|status|restart]"
        echo "       $0 kill   [ray-worker-a|ray-worker-b|ray-head]"
        echo "       $0 revive [ray-worker-a|ray-worker-b|ray-head]"
        ;;
esac
