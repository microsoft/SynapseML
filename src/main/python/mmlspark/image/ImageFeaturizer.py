# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark.image._ImageFeaturizer import _ImageFeaturizer
from pyspark.ml.common import inherit_doc
from pyspark.sql import SparkSession

@inherit_doc
class ImageFeaturizer(_ImageFeaturizer):
    """

    Args:
        SparkSession (SparkSession): The SparkSession that will be used to find the model
        ocation (str): The location of the model, either on local or HDFS
    """
    def setModelLocation(self, location):
        self._java_obj = self._java_obj.setModelLocation(location)
        return self

    def setModel(self, modelSchema):
        session = SparkSession.builder.getOrCreate()
        self._java_obj = self._java_obj.setModel(modelSchema.toJava(session))
        return self

    def setMiniBatchSize(self, size):
        self._java_obj = self._java_obj.setMiniBatchSize(size)
        return self
