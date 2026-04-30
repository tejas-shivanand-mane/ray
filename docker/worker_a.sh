#!/bin/bash
python -c "import ray; print('Ray:', ray.__version__)"
ray start \
    --address=192.168.1.241:6399 \
    --num-cpus=1 \
    --object-store-memory=2000000000 \
    --resources='{"worker_a":1}' \
    --block
