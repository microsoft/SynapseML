# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.isolationforest._IsolationForestModel import _IsolationForestModel
from pyspark.ml.common import inherit_doc


@inherit_doc
class IsolationForestModel(_IsolationForestModel):

    # The generated implementation does not work. Override it to return the java object.
    def getInnerModel(self):
        return self._java_obj.getInnerModel()

    def getOutlierScoreThreshold(self):
        return self.getInnerModel().getOutlierScoreThreshold()