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
    def setModelLocation(self, sparkSession, location):
        jSpark = sparkSession._jsparkSession
        self._java_obj = self._java_obj.setModelLocation(jSpark, location)
        return self

    def setInputCol(self, value):
        self._java_obj.setInputCol(value)
        return self

    def getInputCol(self):
        return self._java_obj.getInputCol()

    def setOutputCol(self, value):
        self._java_obj.setOutputCol(value)
        return self

    def getOutputCol(self):
        return self._java_obj.getOutputCol()
