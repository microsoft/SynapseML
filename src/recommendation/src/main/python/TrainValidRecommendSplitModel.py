# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.common import inherit_doc
from pyspark.ml import Model
from pyspark.ml.tuning import ValidatorParams
from pyspark.ml.util import *


@inherit_doc
class TrainValidRecommendSplitModel(Model, ValidatorParams):

    def __init__(self, bestModel, validationMetrics=[]):
        super(TrainValidRecommendSplitModel, self).__init__()
        #: best model from cross validation
        self.bestModel = bestModel
        #: evaluated validation metrics
        self.validationMetrics = validationMetrics

    def _transform(self, dataset):
        return self.bestModel.transform(dataset)

    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
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
        return TrainValidRecommendSplitModel(bestModel, validationMetrics)

    def recommendForAllUsers(self, numItems):
        return self.bestModel._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numItems):
        return self.bestModel._call_java("recommendForAllItems", numItems)
    #
    # def _transform(self, dataset):
    #     return self.bestModel.transform(dataset)
