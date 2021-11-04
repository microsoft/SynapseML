# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= "3":
    basestring = str

from pyspark.ml.tuning import _ValidatorParams
from pyspark.ml.wrapper import JavaParams
from synapse.ml.recommendation._RankingTrainValidationSplit import _RankingTrainValidationSplit


class RankingTrainValidationSplit(_ValidatorParams, _RankingTrainValidationSplit):
    def __init__(self, **kwargs):
        _RankingTrainValidationSplit.__init__(self, **kwargs)

    def _to_java(self):
        estimator, epms, evaluator = _ValidatorParams._to_java_impl(self)

        _java_obj = JavaParams._new_java_obj(
            "com.microsoft.azure.synapse.ml.recommendation.RankingTrainValidationSplit", self.uid
        )
        _java_obj.setEstimatorParamMaps(epms)
        _java_obj.setEvaluator(evaluator)
        _java_obj.setEstimator(estimator)
        _java_obj.setTrainRatio(self.getTrainRatio())
        _java_obj.setSeed(self.getSeed())
        _java_obj.setItemCol(self.getItemCol())
        _java_obj.setUserCol(self.getUserCol())
        _java_obj.setRatingCol(self.getRatingCol())

        return _java_obj

    def _fit(self, dataset):
        model = self._to_java().fit(dataset._jdf)
        return self._create_model(model)
