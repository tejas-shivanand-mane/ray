#!/bin/bash
python -c "import ray; print('Ray:', ray.__version__)"
ray start --head \
    --port=6399 \
    --ray-client-server-port=10001 \
    --dashboard-host=0.0.0.0 \
    --num-cpus=1 \
    --object-store-memory=2000000000 \
    --resources='{"head":1}' \
    --block
