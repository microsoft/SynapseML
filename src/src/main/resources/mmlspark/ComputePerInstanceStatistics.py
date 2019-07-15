# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *

@inherit_doc
class ComputePerInstanceStatistics(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    Evaluates the given scored dataset with per instance metrics.

    The Regression metrics are:

    - "L1_loss"
    - "L2_loss"

    The Classification metrics are:

    - "log_loss"

    Args:

        evaluationMetric (str): Metric to evaluate models with (default: all)
        labelCol (str): The name of the label column
        scoredLabelsCol (str): Scored labels column name, only required if using SparkML estimators
        scoredProbabilitiesCol (str): Scored probabilities, usually calibrated from raw scores, only required if using SparkML estimators
        scoresCol (str): Scores or raw prediction column name, only required if using SparkML estimators
    """

    @keyword_only
    def __init__(self, evaluationMetric="all", labelCol=None, scoredLabelsCol=None, scoredProbabilitiesCol=None, scoresCol=None):
        super(ComputePerInstanceStatistics, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ComputePerInstanceStatistics")
        self.evaluationMetric = Param(self, "evaluationMetric", "evaluationMetric: Metric to evaluate models with (default: all)")
        self._setDefault(evaluationMetric="all")
        self.labelCol = Param(self, "labelCol", "labelCol: The name of the label column")
        self.scoredLabelsCol = Param(self, "scoredLabelsCol", "scoredLabelsCol: Scored labels column name, only required if using SparkML estimators")
        self.scoredProbabilitiesCol = Param(self, "scoredProbabilitiesCol", "scoredProbabilitiesCol: Scored probabilities, usually calibrated from raw scores, only required if using SparkML estimators")
        self.scoresCol = Param(self, "scoresCol", "scoresCol: Scores or raw prediction column name, only required if using SparkML estimators")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, evaluationMetric="all", labelCol=None, scoredLabelsCol=None, scoredProbabilitiesCol=None, scoresCol=None):
        """
        Set the (keyword only) parameters

        Args:

            evaluationMetric (str): Metric to evaluate models with (default: all)
            labelCol (str): The name of the label column
            scoredLabelsCol (str): Scored labels column name, only required if using SparkML estimators
            scoredProbabilitiesCol (str): Scored probabilities, usually calibrated from raw scores, only required if using SparkML estimators
            scoresCol (str): Scores or raw prediction column name, only required if using SparkML estimators
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setEvaluationMetric(self, value):
        """

        Args:

            evaluationMetric (str): Metric to evaluate models with (default: all)

        """
        self._set(evaluationMetric=value)
        return self


    def getEvaluationMetric(self):
        """

        Returns:

            str: Metric to evaluate models with (default: all)
        """
        return self.getOrDefault(self.evaluationMetric)


    def setLabelCol(self, value):
        """

        Args:

            labelCol (str): The name of the label column

        """
        self._set(labelCol=value)
        return self


    def getLabelCol(self):
        """

        Returns:

            str: The name of the label column
        """
        return self.getOrDefault(self.labelCol)


    def setScoredLabelsCol(self, value):
        """

        Args:

            scoredLabelsCol (str): Scored labels column name, only required if using SparkML estimators

        """
        self._set(scoredLabelsCol=value)
        return self


    def getScoredLabelsCol(self):
        """

        Returns:

            str: Scored labels column name, only required if using SparkML estimators
        """
        return self.getOrDefault(self.scoredLabelsCol)


    def setScoredProbabilitiesCol(self, value):
        """

        Args:

            scoredProbabilitiesCol (str): Scored probabilities, usually calibrated from raw scores, only required if using SparkML estimators

        """
        self._set(scoredProbabilitiesCol=value)
        return self


    def getScoredProbabilitiesCol(self):
        """

        Returns:

            str: Scored probabilities, usually calibrated from raw scores, only required if using SparkML estimators
        """
        return self.getOrDefault(self.scoredProbabilitiesCol)


    def setScoresCol(self, value):
        """

        Args:

            scoresCol (str): Scores or raw prediction column name, only required if using SparkML estimators

        """
        self._set(scoresCol=value)
        return self


    def getScoresCol(self):
        """

        Returns:

            str: Scores or raw prediction column name, only required if using SparkML estimators
        """
        return self.getOrDefault(self.scoresCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.ComputePerInstanceStatistics"

    @staticmethod
    def _from_java(java_stage):
        module_name=ComputePerInstanceStatistics.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ComputePerInstanceStatistics"
        return from_java(java_stage, module_name)
