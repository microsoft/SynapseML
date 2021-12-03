# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml.wrapper import JavaWrapper
from pyspark import SparkContext

class ICECategoricalFeature(JavaWrapper):
    def __init__(self, col: str, numTopValues: int = None, outputColName: str = None):
        sc = SparkContext._active_spark_context
        numTopValues = sc._jvm.scala.Some(numTopValues) if numTopValues else sc._jvm.scala.Option.empty()
        outputColName = sc._jvm.scala.Some(outputColName) if outputColName else sc._jvm.scala.Option.empty()
        self._java_obj = JavaWrapper._new_java_obj("com.microsoft.azure.synapse.ml.explainers.ICECategoricalFeature", col, numTopValues, outputColName)

    def getObject(self):
      return self._java_obj

class ICENumericFeature(JavaWrapper):
    def __init__(self, col: str, numSplits: int = None, rangeMin: float = None, rangeMax: float = None, outputColName: str = None):
        sc = SparkContext._active_spark_context
        numSplits = sc._jvm.scala.Some(numSplits) if numSplits else sc._jvm.scala.Option.empty()
        rangeMin = sc._jvm.scala.Some(rangeMin) if rangeMin else sc._jvm.scala.Option.empty()
        rangeMax = sc._jvm.scala.Some(rangeMax) if rangeMax else sc._jvm.scala.Option.empty()
        outputColName = sc._jvm.scala.Some(outputColName) if outputColName else sc._jvm.scala.Option.empty()
        self._java_obj = JavaWrapper._new_java_obj("com.microsoft.azure.synapse.ml.explainers.ICENumericFeature", col, numSplits, rangeMin, rangeMax, outputColName)


    def getObject(self):
        return self._java_obj
