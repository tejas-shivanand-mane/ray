FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Install prerequisites and deadsnakes PPA
# deadsnakes supports Ubuntu 24.04 (noble) unlike 25.10 (questing)
RUN apt-get update && apt-get install -y \
    software-properties-common \
    curl \
    gcc g++ \
    libstdc++6 \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update && apt-get install -y \
    python3.10 \
    python3.10-dev \
    python3.10-venv \
    python3.10-distutils \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

RUN pip install --upgrade pip --root-user-action=ignore && \
    pip install --root-user-action=ignore \
    numpy psutil aiohttp grpcio cython \
    setproctitle colorama click pyyaml jsonschema \
    msgpack protobuf opencensus prometheus-client \
    requests attrs packaging filelock aiohttp-cors

# Copy host's libstdc++ for GLIBCXX_3.4.34
COPY docker/libstdc++.so.6 /usr/lib/x86_64-linux-gnu/libstdc++.so.6

# Verify versions
RUN ldd --version | head -1
RUN strings /usr/lib/x86_64-linux-gnu/libstdc++.so.6 \
    | grep "GLIBCXX_3.4.3" | tail -3

# Install pre-compiled Ray wheel
COPY docker/ray-3.0.0.dev0-cp310-cp310-linux_x86_64.whl /tmp/
RUN pip install --root-user-action=ignore \
    /tmp/ray-3.0.0.dev0-cp310-cp310-linux_x86_64.whl

# Verify
RUN python -c "import ray; print('Ray OK:', ray.__version__)"

WORKDIR /ray
CMD ["bash"]
