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
        return self._java_obj.getAvgTreatmentEffect()

    def getConfidenceInterval(self):
        return list(self._java_obj.getConfidenceInterval())

    def getPValue(self):
        return self._java_obj.getPValue()
