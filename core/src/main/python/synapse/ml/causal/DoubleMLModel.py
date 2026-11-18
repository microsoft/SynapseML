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

    def getRawTreatmentEffectsByIteration(self):
        """Return a copy of the flat, iteration-major plural coefficient draws.

        Use :meth:`getRawTreatmentEffectRows` for one vector per iteration.
        JVM plural-route and fitted-order checks apply. Python-only parameter
        edits are not transferred to the JVM by convenience getters.
        """
        return list(self._java_obj.getRawTreatmentEffectsByIteration())

    def getRawTreatmentEffectRows(self):
        """Return copied draw rows in the JVM's fitted treatment order."""
        return [list(row) for row in self._java_obj.getRawTreatmentEffectRows()]

    def getAvgTreatmentEffects(self):
        """Return coordinate-wise bootstrap means in fitted treatment order.

        These are means of the bootstrap fits, not a separate full-sample fit.
        Use the singular getter family for a singular-treatment model.
        """
        return list(self._java_obj.getAvgTreatmentEffects())

    def getPValues(self):
        """Return approximate coordinate-wise normal-bootstrap Wald p-values.

        The standard error is the sample SD of draws, not SD divided by the
        square root of their count. At least two draws are required; zero or
        nonfinite SD produces NaN for that coordinate. There is no multiplicity
        correction or joint inference. This differs from singular getPValue.
        """
        return list(self._java_obj.getPValues())

    def getConfidenceIntervals(self):
        """Return copied configured coordinate-wise bootstrap intervals (default 95%).

        These intervals have no joint-coverage or multiplicity guarantee.
        Thirty draws give coarse tails and rough standard errors.
        """
        return [list(interval) for interval in self._java_obj.getConfidenceIntervals()]

    def getFittedTreatmentCols(self):
        """Return a copy of the JVM's persisted fitted treatment order.

        This snapshot is a mutable Spark param, not an immutable identifier.
        Python setters only change local Python state until transferred; these
        convenience methods query JVM state without transferring local params.
        Generated raw getters may raise KeyError on unset/wrong-route params.
        """
        return list(self._java_obj.getFittedTreatmentCols())
