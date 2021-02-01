# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

from pyspark.ml.param.shared import HasSeed
from pyspark.ml.tuning import _ValidatorParams


class ValidatorSetterParams(_ValidatorParams, HasSeed):
    """
    Common params for TrainValidationSplit and CrossValidator.
    """

    def setEstimator(self, value):
        """
        Sets the value of :py:attr:`estimator`.
        """
        return self._set(estimator=value)

    def setEstimatorParamMaps(self, value):
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        return self._set(estimatorParamMaps=value)

    def setEvaluator(self, value):
        """
        Sets the value of :py:attr:`evaluator`.
        """
        return self._set(evaluator=value)
