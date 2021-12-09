# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.explainers._ICETransformer import _ICETransformer
from pyspark.ml.common import inherit_doc
from typing import List, Dict

@inherit_doc
class ICETransformer(_ICETransformer):
    def setCategoricalFeatures(self, value: List[Dict]):
        """
        Args:
        value: The list of dicts with parameters for categorical features to explain.
        """
        self._java_obj.setCategoricalFeatures(value)
        return self

    def setNumericFeatures(self, value: List[Dict]):
        """
        Args:
        value: The list of dicts with parameters for numeric features to explain.
        """
        self._java_obj.setNumericFeatures(value)
        return self
