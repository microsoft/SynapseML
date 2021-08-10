# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from mmlspark.onnx._ONNXModel import _ONNXModel
from pyspark.ml.common import inherit_doc


@inherit_doc
class ONNXModel(_ONNXModel):
    """

    Args:
        SparkSession (SparkSession): The SparkSession that will be used to find the model
        location (str): The location of the model, either on local or HDFS
    """

    def setModelLocation(self, location):
        self._java_obj = self._java_obj.setModelLocation(location)
        return self

    def setMiniBatchSize(self, n):
        self._java_obj = self._java_obj.setMiniBatchSize(n)
        return self

