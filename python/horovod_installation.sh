#!/bin/bash

# This is the script used to install horovod on ubuntu2004 with NCCL on databricks

# exit immediately on failure, or if an undefined variable is used
set -eu

# Install prerequisite libraries that horovod depends on
pip install pytorch-lightning==1.5.0
pip install torchvision==0.12.0

# Remove Outdated Signing Key:
sudo apt-key del 7fa2af80

# Install the new cuda-keyring package:
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.0-1_all.deb
sudo dpkg -i cuda-keyring_1.0-1_all.deb

apt-key adv --fetch-keys http://developer.download.nvidia.com/compute/machine-learning/repos/ubuntu2004/x86_64/7fa2af80.pub
wget https://developer.download.nvidia.com/compute/machine-learning/repos/ubuntu2004/x86_64/nvidia-machine-learning-repo-ubuntu2004_1.0.0-1_amd64.deb
dpkg -i ./nvidia-machine-learning-repo-ubuntu2004_1.0.0-1_amd64.deb


apt-get update
apt-get install --allow-downgrades --no-install-recommends -y \
cuda-nvml-dev-11-0=11.0.167-1 \
cuda-nvcc-11-0=11.0.221-1 \
cuda-cudart-dev-11-0=11.0.221-1 \
cuda-libraries-dev-11-0=11.0.3-1 \
libnccl-dev=2.10.3-1+cuda11.0 \
libcusparse-dev-11-0=11.1.1.245-1

git clone --recursive https://github.com/horovod/horovod.git
cd horovod
# fix a certain commit at version 0.24.3
git reset --hard 7707267a4bef79e09a9df1d41b0652feb61b76c7
rm -rf build/ dist/
HOROVOD_GPU_ALLREDUCE=NCCL HOROVOD_CUDA_HOME=/usr/local/cuda-11/ HOROVOD_WITH_PYTORCH=1 HOROVOD_WITHOUT_MXNET=1 \
/databricks/python3/bin/python setup.py bdist_wheel

readlink -f dist/horovod-*.whl

pip install --no-cache-dir dist/horovod-0.24.3-cp38-cp38-linux_x86_64.whl --force-reinstall --no-deps
