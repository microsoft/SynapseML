# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark.train._TrainRegressor import _TrainRegressor
from mmlspark.train._TrainRegressor import _TrainedRegressorModel
from pyspark.ml.common import inherit_doc
from pyspark.ml.wrapper import JavaParams

@inherit_doc
class TrainRegressor(_TrainRegressor):
    def _create_model(self, java_model):
        model = TrainedRegressorModel()
        model._java_obj = java_model
        model._transfer_params_from_java()
        return model

@inherit_doc
class TrainedRegressorModel(_TrainedRegressorModel):
    def getModel(self):
        """
        Get the underlying model.
        """
        return JavaParams._from_java(self._java_obj.getModel())
