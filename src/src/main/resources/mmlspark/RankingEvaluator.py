# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.evaluation import JavaEvaluator
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *

@inherit_doc
class RankingEvaluator(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEvaluator):
    """


    Args:

        itemCol (str): Column of items
        k (int): number of items (default: 10)
        labelCol (str): label column name (default: label)
        metricName (str): metric name in evaluation (ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp) (default: ndcgAt)
        nItems (long): number of items (default: -1)
        predictionCol (str): prediction column name (default: prediction)
        ratingCol (str): Column of ratings
        userCol (str): Column of users
    """

    @keyword_only
    def __init__(self, itemCol=None, k=10, labelCol="label", metricName="ndcgAt", nItems=-1, predictionCol="prediction", ratingCol=None, userCol=None):
        super(RankingEvaluator, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.RankingEvaluator")
        self.itemCol = Param(self, "itemCol", "itemCol: Column of items")
        self.k = Param(self, "k", "k: number of items (default: 10)")
        self._setDefault(k=10)
        self.labelCol = Param(self, "labelCol", "labelCol: label column name (default: label)")
        self._setDefault(labelCol="label")
        self.metricName = Param(self, "metricName", "metricName: metric name in evaluation (ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp) (default: ndcgAt)")
        self._setDefault(metricName="ndcgAt")
        self.nItems = Param(self, "nItems", "nItems: number of items (default: -1)")
        self._setDefault(nItems=-1)
        self.predictionCol = Param(self, "predictionCol", "predictionCol: prediction column name (default: prediction)")
        self._setDefault(predictionCol="prediction")
        self.ratingCol = Param(self, "ratingCol", "ratingCol: Column of ratings")
        self.userCol = Param(self, "userCol", "userCol: Column of users")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, itemCol=None, k=10, labelCol="label", metricName="ndcgAt", nItems=-1, predictionCol="prediction", ratingCol=None, userCol=None):
        """
        Set the (keyword only) parameters

        Args:

            itemCol (str): Column of items
            k (int): number of items (default: 10)
            labelCol (str): label column name (default: label)
            metricName (str): metric name in evaluation (ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp) (default: ndcgAt)
            nItems (long): number of items (default: -1)
            predictionCol (str): prediction column name (default: prediction)
            ratingCol (str): Column of ratings
            userCol (str): Column of users
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setItemCol(self, value):
        """

        Args:

            itemCol (str): Column of items

        """
        self._set(itemCol=value)
        return self


    def getItemCol(self):
        """

        Returns:

            str: Column of items
        """
        return self.getOrDefault(self.itemCol)


    def setK(self, value):
        """

        Args:

            k (int): number of items (default: 10)

        """
        self._set(k=value)
        return self


    def getK(self):
        """

        Returns:

            int: number of items (default: 10)
        """
        return self.getOrDefault(self.k)


    def setLabelCol(self, value):
        """

        Args:

            labelCol (str): label column name (default: label)

        """
        self._set(labelCol=value)
        return self


    def getLabelCol(self):
        """

        Returns:

            str: label column name (default: label)
        """
        return self.getOrDefault(self.labelCol)


    def setMetricName(self, value):
        """

        Args:

            metricName (str): metric name in evaluation (ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp) (default: ndcgAt)

        """
        self._set(metricName=value)
        return self


    def getMetricName(self):
        """

        Returns:

            str: metric name in evaluation (ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp) (default: ndcgAt)
        """
        return self.getOrDefault(self.metricName)


    def setNItems(self, value):
        """

        Args:

            nItems (long): number of items (default: -1)

        """
        self._set(nItems=value)
        return self


    def getNItems(self):
        """

        Returns:

            long: number of items (default: -1)
        """
        return self.getOrDefault(self.nItems)


    def setPredictionCol(self, value):
        """

        Args:

            predictionCol (str): prediction column name (default: prediction)

        """
        self._set(predictionCol=value)
        return self


    def getPredictionCol(self):
        """

        Returns:

            str: prediction column name (default: prediction)
        """
        return self.getOrDefault(self.predictionCol)


    def setRatingCol(self, value):
        """

        Args:

            ratingCol (str): Column of ratings

        """
        self._set(ratingCol=value)
        return self


    def getRatingCol(self):
        """

        Returns:

            str: Column of ratings
        """
        return self.getOrDefault(self.ratingCol)


    def setUserCol(self, value):
        """

        Args:

            userCol (str): Column of users

        """
        self._set(userCol=value)
        return self


    def getUserCol(self):
        """

        Returns:

            str: Column of users
        """
        return self.getOrDefault(self.userCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.RankingEvaluator"

    @staticmethod
    def _from_java(java_stage):
        module_name=RankingEvaluator.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".RankingEvaluator"
        return from_java(java_stage, module_name)
