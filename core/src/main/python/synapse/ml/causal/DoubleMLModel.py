# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.causal._DoubleMLModel import _DoubleMLModel
from pyspark.ml.common import inherit_doc
import numpy as np


@inherit_doc
class DoubleMLModel(_DoubleMLModel):
    def getAvgTreatmentEffect(self):
        return sum(self.getRawTreatmentEffects()) / len(self.getRawTreatmentEffects())

    def getConfidenceInterval(self):
        ciLowerBound = np.percentile(
            self.getRawTreatmentEffects(), 100 * (1 - self.getConfidenceLevel())
        )
        ciUpperBound = np.percentile(
            self.getRawTreatmentEffects(), self.getConfidenceLevel() * 100
        )
        return [ciLowerBound, ciUpperBound]
