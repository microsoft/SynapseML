# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from synapse.ml.automl._BestModel import _BestModel
from pyspark import SparkContext, SQLContext
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import inherit_doc
from pyspark.sql import DataFrame


@inherit_doc
class BestModel(_BestModel):
    def getBestModel(self):
        """
        Returns the best model.
        """
        return JavaParams._from_java(self._java_obj.getBestModel())

    def getScoredDataset(self):
        """
        Returns scored dataset for the best model.
        """
        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)
        return DataFrame(self._java_obj.getScoredDataset(), sql_ctx)

    def getEvaluationResults(self):
        """
        Returns the ROC curve with TPR, FPR.
        """
        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)
        return DataFrame(self._java_obj.getEvaluationResults(), sql_ctx)

    def getBestModelMetrics(self):
        """
        Returns all of the best model metrics results from the evaluator.
        """
        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)
        return DataFrame(self._java_obj.getBestModelMetrics(), sql_ctx)

    def getAllModelMetrics(self):
        """
        Returns a table of metrics from all models compared from the evaluation comparison.
        """
        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)
        return DataFrame(self._java_obj.getAllModelMetrics(), sql_ctx)
