# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from mmlspark.lightgbm._LightGBMRegressionModel import _LightGBMRegressionModel
from mmlspark.lightgbm.mixin import LightGBMModelMixin
from pyspark import SparkContext
from pyspark.ml.common import inherit_doc
from pyspark.ml.wrapper import JavaParams
from mmlspark.core.serialize.java_params_patch import *

@inherit_doc
class LightGBMRegressionModel(LightGBMModelMixin, _LightGBMRegressionModel):
    @staticmethod
    def loadNativeModelFromFile(filename):
        """
        Load the model from a native LightGBM text file.
        """
        ctx = SparkContext._active_spark_context
        loader = ctx._jvm.com.microsoft.ml.spark.lightgbm.LightGBMRegressionModel
        java_model = loader.loadNativeModelFromFile(filename)
        return JavaParams._from_java(java_model)

    @staticmethod
    def loadNativeModelFromString(model):
        """
        Load the model from a native LightGBM model string.
        """
        ctx = SparkContext._active_spark_context
        loader = ctx._jvm.com.microsoft.ml.spark.lightgbm.LightGBMRegressionModel
        java_model = loader.loadNativeModelFromString(model)
        return JavaParams._from_java(java_model)
