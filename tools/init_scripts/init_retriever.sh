# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

echo "Starting init script execution..." 

RAPIDS_VERSION=23.10.0
SPARK_RAPIDS_VERSION=23.10.0
SPARK_RAPIDSML_VERSION=24.04
NAVIGATOR_CACHE=/dbfs/FileStore/model_navigator.zip
TORCH_CACHE=/dbfs/FileStore/torch.zip

# To eliminate optimization stage 
# Unzip optimized TRT Navigator model and Transformer
# unzip ${NAVIGATOR_CACHE} -d /root/.cache 
# unzip ${TORCH_CACHE} -d /root/.cache 

# install cudatoolkit 11.8 via runfile approach
wget https://developer.download.nvidia.com/compute/cuda/11.8.0/local_installers/cuda_11.8.0_520.61.05_linux.run
sh cuda_11.8.0_520.61.05_linux.run --silent --toolkit

# reset symlink and update library loading paths
rm /usr/local/cuda
ln -s /usr/local/cuda-11.8 /usr/local/cuda

# upgrade pip
/databricks/python/bin/pip install --upgrade pip

# install cudf, cuml and their rapids dependencies
# using ~= pulls in latest micro version patches
/databricks/python/bin/pip install --extra-index-url https://pypi.nvidia.com cudf-cu11~=${RAPIDS_VERSION} cuml-cu11~=${RAPIDS_VERSION} pylibraft-cu11~=${RAPIDS_VERSION} rmm-cu11~=${RAPIDS_VERSION} 

# install model navigator
/databricks/python/bin/pip install --extra-index-url https://pypi.nvidia.com onnxruntime-gpu==1.16.3 "tensorrt==9.3.0.post12.dev1" "triton-model-navigator<1" "sentence_transformers~=2.2.2" "faker" "urllib3<2" 

# install spark-rapids-ml
/databricks/python/bin/pip install spark-rapids-ml~=${SPARK_RAPIDSML_VERSION}

# upgrade grpc
# /databricks/python/bin/pip uninstall -y grpcio grpcio-tools
# Install the specific version of grpcio
/databricks/python/bin/pip install grpcio==1.64.1

echo "Init script execution completed." 