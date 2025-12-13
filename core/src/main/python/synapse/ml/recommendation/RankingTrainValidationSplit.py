# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= "3":
    basestring = str

from pyspark.ml.tuning import _ValidatorParams
from pyspark.ml.wrapper import JavaParams
from synapse.ml.recommendation._RankingTrainValidationSplit import (
    _RankingTrainValidationSplit,
)


class RankingTrainValidationSplit(_ValidatorParams, _RankingTrainValidationSplit):
    """
    Python wrapper which delegates most behavior to the generated
    `_RankingTrainValidationSplit`, but integrates with PySpark's
    `_ValidatorParams` machinery so that `estimatorParamMaps` is always
    available for `_to_java` and for persistence.
    """

    def __init__(self, estimatorParamMaps=None, evaluator=None, **kwargs):
        # Ensure we always have at least one ParamMap so that
        # _ValidatorParams._to_java_impl(self) can call
        # getEstimatorParamMaps() without hitting a KeyError, and so that
        # the Scala side's require(nonEmpty) passes.
        if estimatorParamMaps is None:
            estimatorParamMaps = [dict()]

        # Provide a sensible default evaluator that matches the Scala side
        # expectations if the caller does not supply one explicitly.
        if evaluator is None:
            from synapse.ml.recommendation import RankingEvaluator

            evaluator = RankingEvaluator().setK(3).setNItems(10)

        _RankingTrainValidationSplit.__init__(
            self, estimatorParamMaps=estimatorParamMaps, **kwargs
        )

        # Seed the evaluator Param used by PySpark's _ValidatorParams
        # implementation so that getEvaluator/getOrDefault work correctly
        # under PySpark 4.
        self._set(evaluator=evaluator)

    def getEstimatorParamMaps(self):
        # Delegate to the generated implementation so that there is a single
        # source of truth for this parameter; _ValidatorParams._to_java_impl
        # will call this method.
        return _RankingTrainValidationSplit.getEstimatorParamMaps(self)

    def _to_java(self):
        estimator, epms, evaluator = _ValidatorParams._to_java_impl(self)

        _java_obj = JavaParams._new_java_obj(
            "com.microsoft.azure.synapse.ml.recommendation.RankingTrainValidationSplit",
            self.uid,
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
