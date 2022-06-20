# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

pyspark_require_list = ['pyspark>=3.2.0;python_version>="3.8"']
spark_require_list = [
    "numpy",
    "petastorm>=0.11.0",
    "pyarrow>=0.15.0",
    "fsspec>=2021.07.0",
]  ## TO BE VERIFY
dl_require_list = [
    "torch>=1.11.0",
    "torchvision>=0.12.0",
    "pytorch_lightning>=1.5.0,<1.5.10",
    "horovod>=0.24.3",
]
