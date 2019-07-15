# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys
from mmlspark.recommendation.RankingTrainValidationSplitModel import RankingTrainValidationSplitModel as tvmodel
from mmlspark.recommendation._RankingTrainValidationSplit import _RankingTrainValidationSplit
from pyspark import keyword_only
from pyspark.ml.param import Params
from pyspark.ml.tuning import ValidatorParams
from pyspark.ml.util import *
from pyspark import keyword_only
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaParams
from pyspark.ml import Estimator


if sys.version >= '3':
    basestring = str


@inherit_doc
class RankingTrainValidationSplit(Estimator, ValidatorParams):
    trainRatio = Param(Params._dummy(), "trainRatio", "Param for ratio between train and\
         validation data. Must be between 0 and 1.", typeConverter=TypeConverters.toFloat)
    userCol = Param(Params._dummy(), "userCol",
                    "userCol: column name for user ids. Ids must be within the integer value range. (default: user)")
    ratingCol = Param(Params._dummy(), "ratingCol", "ratingCol: column name for ratings (default: rating)")

    itemCol = Param(Params._dummy(), "itemCol",
                    "itemCol: column name for item ids. Ids must be within the integer value range. (default: item)")

    def setTrainRatio(self, value):
        """
        Sets the value of :py:attr:`trainRatio`.
        """
        return self._set(trainRatio=value)

    def getTrainRatio(self):
        """
        Gets the value of trainRatio or its default value.
        """
        return self.getOrDefault(self.trainRatio)

    def setItemCol(self, value):
        """

        Args:

            itemCol (str): column name for item ids. Ids must be within the integer value range. (default: item)

        """
        self._set(itemCol=value)
        return self

    def getItemCol(self):
        """

        Returns:

            str: column name for item ids. Ids must be within the integer value range. (default: item)
        """
        return self.getOrDefault(self.itemCol)

    def setRatingCol(self, value):
        """

        Args:

            ratingCol (str): column name for ratings (default: rating)

        """
        self._set(ratingCol=value)
        return self

    def getRatingCol(self):
        """

        Returns:

            str: column name for ratings (default: rating)
        """
        return self.getOrDefault(self.ratingCol)

    def setUserCol(self, value):
        """

        Args:

            userCol (str): column name for user ids. Ids must be within the integer value range. (default: user)

        """
        self._set(userCol=value)
        return self

    def getUserCol(self):
        """

        Returns:

            str: column name for user ids. Ids must be within the integer value range. (default: user)
        """
        return self.getOrDefault(self.userCol)
    @keyword_only
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, seed=None):
        """
        __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                 seed=None)
        """
        super(RankingTrainValidationSplit, self).__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    def setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, seed=None):
        """
        setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                  seed=None):
        Sets com.microsoft.ml.spark.core.serialize.params for cross validator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra com.microsoft.ml.spark.core.serialize.params. This copies creates a deep copy of
        the embedded paramMap, and copies the embedded and extra parameters over.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        newCV = Params.copy(self, extra)
        if self.isSet(self.estimator):
            newCV.setEstimator(self.getEstimator().copy(extra))
        # estimatorParamMaps remain the same
        if self.isSet(self.evaluator):
            newCV.setEvaluator(self.getEvaluator().copy(extra))
        return newCV

    def _create_model(self, java_model):
        from mmlspark.recommendation.RankingTrainValidationSplitModel import RankingTrainValidationSplitModel
        model = RankingTrainValidationSplitModel._from_java(java_model)
        return model

    def _to_java(self):
        """
        Transfer this instance to a Java TrainValidationSplit. Used for ML persistence.
        :return: Java object equivalent to this instance.
        """

        estimator, epms, evaluator = super(RankingTrainValidationSplit, self)._to_java_impl()

        _java_obj = JavaParams._new_java_obj("com.microsoft.ml.spark.recommendation.RankingTrainValidationSplit",
                                             self.uid)
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
