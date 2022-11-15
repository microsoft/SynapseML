# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.onnx._ImageFeaturizer import _ImageFeaturizer
from pyspark.ml.common import inherit_doc


@inherit_doc
class ImageFeaturizer(_ImageFeaturizer):
    """

    Args:
        name (str): The name of the model in the OnnxHub
        location (str): The location of the model, either on local or HDFS
    """

    def setModelLocation(self, location):
        self._java_obj = self._java_obj.setModelLocation(location)
        return self

    def setModel(self, name):
        self._java_obj = self._java_obj.setModel(name)
        return self

    def setMiniBatchSize(self, size):
        self._java_obj = self._java_obj.setMiniBatchSize(size)
        return self
