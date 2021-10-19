# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitClassifier import _VowpalWabbitClassifier
from pyspark.ml.common import inherit_doc

@inherit_doc
class VowpalWabbitClassifier(_VowpalWabbitClassifier):

    def setInitialModel(self, model):
        """
        Initialize the estimator with a previously trained model.
        """
        self._java_obj.setInitialModel(model._java_obj.getModel())
