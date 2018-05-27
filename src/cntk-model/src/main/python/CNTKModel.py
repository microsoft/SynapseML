# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark._CNTKModel import _CNTKModel
from pyspark.ml.common import inherit_doc


@inherit_doc
class CNTKModel(_CNTKModel):
    """

    Args:
        SparkSession (SparkSession): The SparkSession that will be used to find the model
        location (str): The location of the model, either on local or HDFS
    """

    def setModelLocation(self, location):
        self._java_obj = self._java_obj.setModelLocation(location)
        return self

    def rebroadcastCNTKModel(self, sparkSession):
        jSpark = sparkSession._jsparkSession
        self._java_obj = self._java_obj.rebroadcastCNTKModel(jSpark)

    def setMiniBatchSize(self, n):
        self._java_obj = self._java_obj.setMiniBatchSize(n)
        return self

    def setInputNodeIndex(self, n):
        self._java_obj = self._java_obj.setInputNodeIndex(n)
        return self

    def getInputNodeIndex(self):
        return self._java_obj.getInputNodeIndex()

    def setInputNode(self, n):
        self._java_obj = self._java_obj.setInputNode(n)
        return self

    def getInputNode(self):
        return self._java_obj.getInputNode()

    def setInputCol(self, n):
        self._java_obj = self._java_obj.setInputCol(n)
        return self

    def getInputCol(self):
        return self._java_obj.getInputCol()

    def setOutputNodeIndex(self, n):
        self._java_obj = self._java_obj.setOutputNodeIndex(n)
        return self

    def getOutputNodeIndex(self):
        return self._java_obj.getOutputNodeIndex()

    def setOutputNode(self, n):
        self._java_obj = self._java_obj.setOutputNode(n)
        return self

    def getOutputNode(self):
        return self._java_obj.getOutputNode()

    def setOutputCol(self, n):
        self._java_obj = self._java_obj.setOutputCol(n)
        return self

    def getOutputCol(self):
        return self._java_obj.getOutputCol()

    def getInputShapes(self):
        return self._java_obj.getInputShapes()
