#!/bin/bash
echo "================================================"
echo "Ray Rebuild + Docker Restart"
echo "================================================"

# Step 1: Recompile Ray C++
echo "Step 1: Recompiling Ray..."
cd ~/work/ray
conda run -n ray-dev pip install -e python/ 2>&1 | tail -5

# Step 2: Build new wheel
echo "Step 2: Building wheel..."
conda run -n ray-dev pip wheel python/ \
    -w /tmp/ray-wheel/ --no-deps 2>&1 | tail -3
cp /tmp/ray-wheel/ray-*.whl ~/work/ray/docker/

# Step 3: Rebuild Docker image
echo "Step 3: Rebuilding Docker image..."
cd ~/work/ray
docker build -t ray-custom:latest \
    -f docker/Dockerfile . 2>&1 | tail -5

# Step 4: Restart cluster
echo "Step 4: Restarting cluster..."
~/ray-docker-cluster.sh restart

echo "================================================"
echo "Done — cluster ready with new code"
echo "================================================"
