# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.core.schema.Utils import *
from synapse.ml.stages._LumpFeaturesModel import _LumpFeaturesModel


@inherit_doc
class LumpFeaturesModel(_LumpFeaturesModel):
    def getKeptValues(self):
        """Returns the values LumpFeatures learned to retain, per rule column.

        Every value not listed here is replaced with otherValue at transform time, so this is the
        authoritative view of what the fitted model will keep. Use it to audit a fit before scoring:
        a column whose list is much shorter than its top-K means the frequency filters (minCount,
        minFreq) removed the tail, and an empty list means every value gets lumped.

        Returns:

            dict: map from rule column name to its retained values, ordered by descending
                frequency in the fitting data and then ascending value.
        """
        return json.loads(self._call_java("getKeptValuesAsJson"))
