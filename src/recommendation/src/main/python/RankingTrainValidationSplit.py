# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from mmlspark.Utils import *
from pyspark.ml.common import inherit_doc
from pyspark.ml import Model
from pyspark.ml.util import *

import sys
from mmlspark._RankingTrainValidationSplit import _RankingTrainValidationSplit, _RankingTrainValidationSplitModel
from pyspark.ml import Estimator
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.tuning import ValidatorParams
from pyspark.ml.util import *
from pyspark.ml.param.shared import HasParallelism

if sys.version >= '3':
    basestring = str


class HasCollectSubMetrics(Params):
    """
    Mixin for param collectSubModels: Param for whether to collect a list of sub-models trained during tuning. If set to false, then only the single best sub-model will be available after fitting. If set to true, then all sub-models will be available. Warning: For large models, collecting all sub-models can cause OOMs on the Spark driver.
    """

    collectSubMetrics = Param(Params._dummy(), "collectSubMetrics",
                              "Param for whether to collect a list of sub-models metrics.",
                              typeConverter=TypeConverters.toBoolean)

    def __init__(self):
        super(HasCollectSubMetrics, self).__init__()
        self._setDefault(collectSubMetrics=False)

    def setCollectSubMetrics(self, value):
        """
        Sets the value of :py:attr:`collectSubModels`.
        """
        return self._set(collectSubModels=value)

    def getCollectSubMetrics(self):
        """
        Gets the value of collectSubModels or its default value.
        """
        return self.getOrDefault(self.collectSubMetrics)


class HasCollectSubModels(Params):
    """
    Mixin for param collectSubModels: Param for whether to collect a list of sub-models trained during tuning. If set to false, then only the single best sub-model will be available after fitting. If set to true, then all sub-models will be available. Warning: For large models, collecting all sub-models can cause OOMs on the Spark driver.
    """

    collectSubModels = Param(Params._dummy(), "collectSubModels",
                             "Param for whether to collect a list of sub-models trained during tuning. If set to false, then only the single best sub-model will be available after fitting. If set to true, then all sub-models will be available. Warning: For large models, collecting all sub-models can cause OOMs on the Spark driver.",
                             typeConverter=TypeConverters.toBoolean)

    def __init__(self):
        super(HasCollectSubModels, self).__init__()
        self._setDefault(collectSubModels=False)

    def setCollectSubModels(self, value):
        """
        Sets the value of :py:attr:`collectSubModels`.
        """
        return self._set(collectSubModels=value)

    def getCollectSubModels(self):
        """
        Gets the value of collectSubModels or its default value.
        """
        return self.getOrDefault(self.collectSubModels)


@inherit_doc
class RankingTrainValidationSplit(_RankingTrainValidationSplit, Estimator, ValidatorParams, HasCollectSubModels,
                                  HasCollectSubMetrics,
                                  HasParallelism):

    def _create_model(self, java_model):
        model = RankingTrainValidationSplitModel()
        model._java_obj = java_model
        model._transfer_params_from_java()
        return model

    def _fit(self, dataset):
        return self._to_java().fit(dataset._jdf)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java TrainValidationSplit, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        estimator, epms, evaluator = super(RankingTrainValidationSplit, cls)._from_java_impl(java_stage)
        trainRatio = java_stage.getTrainRatio()
        seed = java_stage.getSeed()
        parallelism = java_stage.getParallelism()
        collectSubModels = java_stage.getCollectSubModels()
        collectSubMetrics = java_stage.getCollectSubMetrics()
        # Create a new instance of this stage.
        py_stage = cls(estimator=estimator, estimatorParamMaps=epms, evaluator=evaluator,
                       trainRatio=trainRatio, seed=seed, parallelism=parallelism,
                       collectSubModels=collectSubModels, collectSubMetrics=collectSubMetrics)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java TrainValidationSplit. Used for ML persistence.
        :return: Java object equivalent to this instance.
        """

        estimator, epms, evaluator = super(RankingTrainValidationSplit, self)._to_java_impl()

        _java_obj = JavaParams._new_java_obj("com.microsoft.ml.spark.RankingTrainValidationSplit",
                                             self.uid)
        _java_obj.setEstimatorParamMaps(epms)
        _java_obj.setEvaluator(evaluator)
        _java_obj.setEstimator(estimator)
        _java_obj.setTrainRatio(self.getTrainRatio())
        _java_obj.setSeed(self.getSeed())
        _java_obj.setParallelism(self.getParallelism())
        _java_obj.setCollectSubModels(self.getCollectSubModels())
        _java_obj.setCollectSubMetrics(self.getCollectSubMetrics())
        return _java_obj


@inherit_doc
class RankingTrainValidationSplitModel(_RankingTrainValidationSplitModel, Model, ValidatorParams):

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
        subModels = self.subModels
        subMetrics = self.subMetrics
        return RankingTrainValidationSplitModel(bestModel, validationMetrics, subModels, subMetrics)

    def recommendForAllUsers(self, numItems):
        # return self.bestModel.recommendForAllUsers(numItems)
        return self.bestModel._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numItems):
        return self.bestModel.recommendForAllItems(numItems)

    @classmethod
    def from_java(cls, java_stage):
        """
        Given a Java TrainValidationSplitModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        # Load information from java_stage to the instance.
        bestModel = JavaParams._from_java(java_stage.bestModel())
        estimator, epms, evaluator = super(RankingTrainValidationSplitModel,
                                           cls)._from_java_impl(java_stage)
        # Create a new instance of this stage.
        py_stage = cls(bestModel=bestModel).setEstimator(estimator)
        py_stage = py_stage.setEstimatorParamMaps(epms).setEvaluator(evaluator)

        if java_stage.hasSubModels():
            py_stage.subModels = [JavaParams._from_java(sub_model)
                                  for sub_model in java_stage.subModels()]

        if java_stage.hasSubMetrics():
            py_stage.subMetrics = [JavaParams._from_java(sub_metrics)
                                   for sub_metrics in java_stage.subMetrics()]

        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java TrainValidationSplitModel. Used for ML persistence.
        :return: Java object equivalent to this instance.
        """

        sc = SparkContext._active_spark_context
        # TODO: persst validation metrics as well
        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.tuning.TrainValidationSplitModel",
            self.uid,
            self.bestModel._to_java(),
            _py2java(sc, []))
        estimator, epms, evaluator = super(TrainValidationSplitModel, self)._to_java_impl()

        _java_obj.set("evaluator", evaluator)
        _java_obj.set("estimator", estimator)
        _java_obj.set("estimatorParamMaps", epms)

        if self.subModels is not None:
            java_sub_models = [sub_model._to_java() for sub_model in self.subModels]
            _java_obj.setSubModels(java_sub_models)

        if self.subMetrics is not None:
            java_sub_metrics = [sub_metrics._to_java() for sub_metrics in self.subMetrics]
            _java_obj.setSubMetrics(java_sub_metrics)

        return _java_obj
