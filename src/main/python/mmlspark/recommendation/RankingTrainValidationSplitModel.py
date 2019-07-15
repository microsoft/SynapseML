# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.common import inherit_doc
from pyspark.ml.tuning import ValidatorParams
from pyspark.ml.util import *
from mmlspark.recommendation._RankingTrainValidationSplitModel import _RankingTrainValidationSplitModel
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.util import *
from pyspark.ml.common import _py2java

# Load information from java_stage to the instance.
@inherit_doc
class RankingTrainValidationSplitModel(_RankingTrainValidationSplitModel, ValidatorParams):

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

    def recommendForAllItems(self, numItems):
        return self.bestModel._call_java("recommendForAllItems", numItems)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java TrainValidationSplitModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        # Load information from java_stage to the instance.
        bestModel = JavaParams._from_java(java_stage.getBestModel())
        estimator, epms, evaluator = super(RankingTrainValidationSplitModel,
                                           cls)._from_java_impl(java_stage)
        # Create a new instance of this stage.
        py_stage = cls(bestModel=bestModel).setEstimator(estimator)
        py_stage = py_stage.setEstimatorParamMaps(epms).setEvaluator(evaluator)

        py_stage._resetUid(java_stage.uid())
        return py_stage
