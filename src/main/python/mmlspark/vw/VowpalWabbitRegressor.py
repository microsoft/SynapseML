# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from mmlspark.vw._VowpalWabbitRegressor import _VowpalWabbitRegressor, _VowpalWabbitRegressorModel
from pyspark.ml.common import inherit_doc

@inherit_doc
class VowpalWabbitRegressor(_VowpalWabbitRegressor):
    pass

@inherit_doc
class VowpalWabbitRegressorModel(_VowpalWabbitRegressorModel):
    def saveNativeModel(self, filename):
        """
        Save the native model to a local or WASB remote location.
        """
        self._java_obj.saveNativeModel(filename)