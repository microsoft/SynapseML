# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitClassificationModel import _VowpalWabbitClassificationModel
from pyspark.ml.common import inherit_doc
from pyspark import SparkContext, SQLContext
from pyspark.sql import DataFrame

@inherit_doc
class VowpalWabbitClassificationModel(_VowpalWabbitClassificationModel):
    def saveNativeModel(self, filename):
        """
        Save the native model to a local or WASB remote location.
        """
        self._java_obj.saveNativeModel(filename)

    def getNativeModel(self):
        """
        Get the binary native VW model.
        """
        return self._java_obj.getModel()

    def getReadableModel(self):
        return self._java_obj.getReadableModel()

    def getPerformanceStatistics(self):
        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)
        return DataFrame(self._java_obj.getPerformanceStatistics(), sql_ctx)
