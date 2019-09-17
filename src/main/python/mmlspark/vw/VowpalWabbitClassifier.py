# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from mmlspark.vw._VowpalWabbitClassifier import _VowpalWabbitClassifier, _VowpalWabbitClassificationModel
from pyspark.ml.common import inherit_doc

@inherit_doc
class VowpalWabbitClassifier(_VowpalWabbitClassifier):
    def _create_model(self, java_model):
        model = VowpalWabbitClassificationModel()
        model._java_obj = java_model
        model._transfer_params_from_java()
        return model

@inherit_doc
class VowpalWabbitClassificationModel(_VowpalWabbitClassificationModel):
    def saveNativeModel(self, filename):
        """
        Save the native model to a local or WASB remote location.
        """
        self._java_obj.saveNativeModel(filename)