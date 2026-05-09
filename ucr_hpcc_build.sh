#!/bin/bash
source ~/.bashrc
export PATH=/rhome/tmane002/.conda/envs/ray-dev/bin:$PATH

cd ~/work/ray
RAY_BAZEL_BUILD_OPTIONS="--jobs=4" pip install -e python/ 2>&1 | tee ~/ray-build.log | grep -E "^\[|ERROR|fatal|Killed"
