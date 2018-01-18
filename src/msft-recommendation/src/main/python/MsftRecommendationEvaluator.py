# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= '3':
    basestring = str

from pyspark import keyword_only
from pyspark.ml.common import inherit_doc
from pyspark.ml.evaluation import JavaEvaluator
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasLabelCol, HasPredictionCol
from pyspark.ml.util import JavaMLReadable, JavaMLWritable


@inherit_doc
class MsftRecommendationEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol, JavaMLReadable, JavaMLWritable):
    metricName = Param(Params._dummy(), "metricName",
                       """metric name in evaluation - one of:
                       map - 
                       ndcgAt - 
                       mapk - 
                       recallAtK - 
                       diversityAtK - 
                       maxDiversity - """,
                       typeConverter=TypeConverters.toString)

    # todo: Should not need this, but not sure how to remove
    labelCol = Param(Params._dummy(), "labelCol",
                     """labelCol""",
                     typeConverter=TypeConverters.toString)

    rawPredictionCol = Param(Params._dummy(), "rawPredictionCol",
                             """rawPredictionCol""",
                             typeConverter=TypeConverters.toString)

    k = Param(Params._dummy(), "k",
              """k""",
              typeConverter=TypeConverters.toInt)
    saveAll = Param(Params._dummy(), "saveAll",
                    """saveAll""",
                    typeConverter=TypeConverters.toBoolean)

    nItems = Param(Params._dummy(), "nItems",
                   """number of items""",
                   typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, rawPredictionCol="rawPrediction", labelCol="label", metricName="ndcgAt", k=3, saveAll=False):
        """
        __init__(self, rawPredictionCol="rawPrediction", labelCol="label", \
                 metricName="ndcgAt")
        """
        super(MsftRecommendationEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "com.microsoft.ml.spark.MsftRecommendationEvaluator", self.uid)
        self._setDefault(metricName="ndcgAt")
        self._setDefault(k=3)
        self._setDefault(saveAll=False)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def setRawPredictionCol(self, value):
        """
        Sets the value of :py:attr:`rawPredictionCol`.
        """
        return self._set(rawPredictionCol=value)

    def getRawPredictionCol(self):
        """
        Gets the value of rawPredictionCol or its default value.
        """
        return self.getOrDefault(self.rawPredictionCol)

    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    def setK(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(k=value)

    def getK(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.k)

    def setSaveAll(self, value):
        """
        Sets the value of :py:attr:`saveAll`.
        """

        return self._set(saveAll=value)

    def getSaveAll(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.saveAll)

    def setNumberItems(self, value):
        """
        Sets the value of :py:attr:`nItems`.
        """
        return self._set(nItems=value)

    @keyword_only
    def setParams(self, rawPredictionCol="rawPrediction", labelCol="label", metricName="ndcgAt"):
        """
        setParams(self, rawPredictionCol="rawPrediction", labelCol="label", \
                  metricName="ndcgAt")
        Sets params for binary classification evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)
