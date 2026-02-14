# SynapseML CI Container Image
# Pre-bakes all build dependencies so CI jobs start with a warm environment.
# Rebuilt automatically by BuildCIImage when dependency files change.

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Temurin JDK 8 + system packages in a single layer to avoid redundant apt-get update.
# JDK 8 required — JDK 11 has different CMYK JPEG handling in ImageIO.
# Audio libs (libasound2, libpulse0) needed by Azure Speech SDK.
# libssl1.1 needed by Azure Speech SDK (Ubuntu 22.04 ships OpenSSL 3.0 but SDK requires 1.x).
RUN apt-get update && apt-get install -y --no-install-recommends curl wget git ca-certificates gnupg2 \
    && wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(. /etc/os-release && echo $VERSION_CODENAME) main" \
         > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update && apt-get install -y --no-install-recommends \
         temurin-8-jdk \
         openmpi-bin libopenmpi-dev \
         ffmpeg libgstreamer1.0-0 \
         gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-ugly \
         libasound2 libpulse0 \
         graphviz doxygen \
         build-essential cmake \
         libssl-dev libffi-dev \
         sudo \
    && rm -rf /var/lib/apt/lists/* \
    && wget -q http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2.24_amd64.deb -O /tmp/libssl1.1.deb \
    && dpkg -i /tmp/libssl1.1.deb \
    && rm /tmp/libssl1.1.deb

ENV JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64

# Miniconda
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm /tmp/miniconda.sh \
    && /opt/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main \
    && /opt/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r
ENV PATH=/opt/conda/bin:$PATH
ENV CONDA_CACHE_DIR=/opt/conda/envs

# Conda environment from environment.yml
# Use PIP_NO_CACHE_DIR to save disk; CI is CPU-only so replace CUDA torch with CPU variant
# Direct wheel URLs avoid --extra-index-url which violates CFS policy
COPY environment.yml /tmp/environment.yml
RUN PIP_NO_CACHE_DIR=1 conda env create -f /tmp/environment.yml \
    && conda clean --all -y \
    && rm /tmp/environment.yml \
    && /opt/conda/envs/synapseml/bin/pip install --no-cache-dir --no-deps \
         "https://download.pytorch.org/whl/cpu/torch-2.1.0%2Bcpu-cp311-cp311-linux_x86_64.whl" \
         "https://download.pytorch.org/whl/cpu/torchvision-0.16.0%2Bcpu-cp311-cp311-linux_x86_64.whl" \
    && /opt/conda/envs/synapseml/bin/pip uninstall -y triton \
         nvidia-cublas-cu12 nvidia-cuda-cupti-cu12 nvidia-cuda-nvrtc-cu12 \
         nvidia-cuda-runtime-cu12 nvidia-cudnn-cu12 nvidia-cufft-cu12 \
         nvidia-curand-cu12 nvidia-cusolver-cu12 nvidia-cusparse-cu12 \
         nvidia-nccl-cu12 nvidia-nvjitlink-cu12 nvidia-nvtx-cu12 2>/dev/null || true \
    && chmod -R 777 /opt/conda/envs

# Azure CLI (installed into base python, not conda env)
RUN pip install --no-cache-dir azure-cli==2.60.0

# Spark (pre-downloaded for R tests) — placed before SBT warmup so it's cached
# even when build.sbt changes
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q "https://mmlspark.blob.core.windows.net/installers/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH=${SPARK_HOME}/bin:$PATH

# Node.js 16 (for website deployment)
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g yarn \
    && rm -rf /var/lib/apt/lists/*

# SBT — set COURSIER_CACHE to shared location accessible by any UID (ADO uses UID 1001)
ENV SBT_VERSION=1.10.11
ENV COURSIER_CACHE=/opt/.cache/coursier
RUN wget -q "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" -O /tmp/sbt.tgz \
    && tar -xzf /tmp/sbt.tgz -C /opt \
    && rm /tmp/sbt.tgz \
    && mkdir -p $COURSIER_CACHE
ENV PATH=/opt/sbt/bin:$PATH

# Pre-fetch SBT plugins, all project dependency JARs, and compiler-bridge.
# Copy the full project/ dir and build files so SBT can resolve the complete
# dependency graph. This eliminates ~286 JAR downloads + compiler-bridge
# compilation (~2-3 min) from every test job.
COPY project/               /tmp/sbt-warmup/project/
COPY build.sbt              /tmp/sbt-warmup/build.sbt
COPY sonatype.sbt           /tmp/sbt-warmup/sonatype.sbt
RUN cd /tmp/sbt-warmup \
    && sbt --batch -Dsbt.supershell=false "update; Test/update" 2>/dev/null || true \
    && rm -rf /tmp/sbt-warmup /tmp/.sbt \
    && chmod -R 777 $COURSIER_CACHE

# Pre-download test datasets (static tarball, ~50MB) to avoid downloading in every job
ENV DATASET_CACHE=/opt/datasets
RUN mkdir -p $DATASET_CACHE \
    && wget -q "https://mmlspark.blob.core.windows.net/installers/datasets-2023-04-03.tgz" \
         -O "$DATASET_CACHE/datasets-2023-04-03.tgz"

# No ENTRYPOINT — ADO agent needs to control the process
CMD ["bash"]
