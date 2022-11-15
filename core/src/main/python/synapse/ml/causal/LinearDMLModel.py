# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.causal._LinearDMLModel import _LinearDMLModel
from pyspark.ml.common import inherit_doc
import numpy as np


@inherit_doc
class LinearDMLModel(_LinearDMLModel):
    def getAte(self):
        return sum(self.getAtes()) / len(self.getAtes())

    def getCi(self):
        ciLowerBound = np.percentile(
            self.getAtes(), 100 * (1 - self.getConfidenceLevel())
        )
        ciUpperBound = np.percentile(self.getAtes(), self.getConfidenceLevel() * 100)
        return [ciLowerBound, ciUpperBound]
