# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml.common import inherit_doc
from synapse.ml.stages._EnsembleByKey import _EnsembleByKey


@inherit_doc
class EnsembleByKey(_EnsembleByKey):
    def getColNames(self):
        if self.isSet(self.colNames):
            return self.getOrDefault(self.colNames)
        return [f"{self.getStrategy()}({name})" for name in self.getCols()]
