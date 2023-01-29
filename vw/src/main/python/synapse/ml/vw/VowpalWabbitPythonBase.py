# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml.common import inherit_doc
from pyspark import SparkContext, SQLContext
from pyspark.sql import DataFrame

from pyspark.ml.wrapper import JavaWrapper
from pyspark.ml.common import _py2java


def to_java_params(sc, model, pyParamMap):
    paramMap = JavaWrapper._new_java_obj("org.apache.spark.ml.param.ParamMap")
    for param, value in pyParamMap.items():
        java_param = model._java_obj.getParam(param.name)
        java_value = _py2java(sc, value)
        paramMap.put([java_param.w(java_value)])
    return paramMap


class VowpalWabbitPythonBase:
    def setInitialModel(self, model):
        """
        Initialize the estimator with a previously trained model.
        """
        self._java_obj.setInitialModel(model._java_obj.getModel())

    def parallelFit(self, dataset, param_maps):
        sc = SparkContext._active_spark_context
        self._transfer_params_to_java()
        javaParamMaps = [to_java_params(sc, self, x) for x in param_maps]
        javaModels = self._java_obj.parallelFit(dataset._jdf, javaParamMaps)
        return [self._copyValues(self._create_model(x)) for x in javaModels]


class VowpalWabbitPythonBaseModel:
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
