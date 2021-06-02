# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark.cntk._CNTKModel import _CNTKModel
from pyspark.ml.common import inherit_doc, _java2py, _py2java
from pyspark import SparkContext

@inherit_doc
class CNTKModel(_CNTKModel):
    """

    Args:
        SparkSession (SparkSession): The SparkSession that will be used to find the model
        location (str): The location of the model, either on local or HDFS
    """
    def _transfer_map_from_java(self, param):
        """
        Transforms the embedded com.microsoft.ml.spark.core.serialize.params from the companion Java object.
        """
        sc = SparkContext._active_spark_context
        if self._java_obj.hasParam(param.name):
            java_param = self._java_obj.getParam(param.name)
            if self._java_obj.isSet(java_param):
                java_map = self._java_obj.getOrDefault(java_param).toList()
                pairs = [java_map.apply(i) for i in range(java_map.length())]
                py_map = {p._1():p._2() for p in pairs}
                self._set(**{param.name: py_map})

    def _transfer_map_to_java(self, param):
        """
        Transforms the embedded com.microsoft.ml.spark.core.serialize.params to the companion Java object.
        """
        value = self.extractParamMap()[param]
        java_param = self._java_obj.getParam(param.name)
        pair = java_param.w(value)
        self._java_obj.set(pair)

    def _updatePythonFeedDict(self):
        self._transfer_map_from_java(self.getParam("feedDict"))

    def _updatePythonFetchDict(self):
        self._transfer_map_from_java(self.getParam("fetchDict"))

    def _updateJavaFeedDict(self):
        self._transfer_map_to_java(self.getParam("feedDict"))

    def _updateJavaFetchDict(self):
        self._transfer_map_to_java(self.getParam("fetchDict"))

    def setFeedDict(self, dict):
        super(CNTKModel, self).setFeedDict(dict)
        self._updateJavaFeedDict()
        return self

    def setFetchDict(self, dict):
        super(CNTKModel, self).setFetchDict(dict)
        self._updateJavaFetchDict()
        return self

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
        self._updatePythonFeedDict()
        return self

    def getInputNodeIndex(self):
        return self._java_obj.getInputNodeIndex()

    def setInputNode(self, n):
        self._java_obj = self._java_obj.setInputNode(n)
        self._updatePythonFeedDict()
        return self

    def getInputNode(self):
        return self._java_obj.getInputNode()

    def setInputCol(self, n):
        self._java_obj = self._java_obj.setInputCol(n)
        self._updatePythonFeedDict()
        return self

    def getInputCol(self):
        return self._java_obj.getInputCol()

    def setOutputNodeIndex(self, n):
        self._java_obj = self._java_obj.setOutputNodeIndex(n)
        self._updatePythonFetchDict()
        return self

    def getOutputNodeIndex(self):
        return self._java_obj.getOutputNodeIndex()

    def setOutputNode(self, n):
        self._java_obj = self._java_obj.setOutputNode(n)
        self._updatePythonFetchDict()
        return self

    def getOutputNode(self):
        return self._java_obj.getOutputNode()

    def setOutputCol(self, n):
        self._java_obj = self._java_obj.setOutputCol(n)
        self._updatePythonFetchDict()
        return self

    def getOutputCol(self):
        return self._java_obj.getOutputCol()

    def getInputShapes(self):
        return self._java_obj.getInputShapes()
