# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitRegressor import _VowpalWabbitRegressor
from pyspark.ml.common import inherit_doc


@inherit_doc
class VowpalWabbitRegressor(_VowpalWabbitRegressor):

    def setInitialModel(self, model):
        """
        Initialize the estimator with a previously trained model.
        """
        self._java_obj.setInitialModel(model._java_obj.getModel())
