# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= "3":
    basestring = str

from mmlspark.recommendation._RankingTrainValidationSplitModel import _RankingTrainValidationSplitModel
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.util import *

from mmlspark.recommendation.ValidatorSetterParams import ValidatorSetterParams


# Load information from java_stage to the instance.
@inherit_doc
class RankingTrainValidationSplitModel(_RankingTrainValidationSplitModel, ValidatorSetterParams):
    def __init__(self, bestModel=None, validationMetrics=[]):
        super(RankingTrainValidationSplitModel, self).__init__()
        #: best model from cross validation
        self.bestModel = bestModel
        #: evaluated validation metrics
        self.validationMetrics = validationMetrics

    def _transform(self, dataset):
        return self.bestModel.transform(dataset)

    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra com.microsoft.ml.spark.core.serialize.params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.
        And, this creates a shallow copy of the validationMetrics.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        validationMetrics = list(self.validationMetrics)
        return RankingTrainValidationSplitModel(bestModel, validationMetrics)

    def recommendForAllUsers(self, numItems):
        return self.bestModel._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numUsers):
        return self.bestModel._call_java("recommendForAllItems", numUsers)

    def setEstimator(self, value):
        """
        Sets the value of :py:attr:`estimator`.
        """
        return self._set(estimator=value)

    def setEvaluator(self, value):
        """
        Sets the value of :py:attr:`evaluator`.
        """
        return self._set(evaluator=value)

    def setEstimatorParamMaps(self, value):
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        return self._set(estimatorParamMaps=value)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java TrainValidationSplitModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        bestModel = JavaParams._from_java(java_stage.getBestModel())
        py_stage = cls(bestModel=bestModel)

        py_stage._resetUid(java_stage.uid())
        return py_stage
