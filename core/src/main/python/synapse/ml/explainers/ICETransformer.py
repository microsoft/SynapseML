# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.explainers._ICETransformer import _ICETransformer
from pyspark.ml.common import inherit_doc
from typing import List, Dict, Union


@inherit_doc
class ICETransformer(_ICETransformer):
    def setCategoricalFeatures(self, values: List[Union[str, Dict]]):
        """
        Args:
        values: The list of values that represent categorical features to explain.
        Values are list of dicts with parameters or just a list of names of categorical features
        """
        if len(values) == 0:
            pass
        else:
            list_values = []
            for value in values:
                if isinstance(value, str):
                    list_values.append({"name": value})
                elif isinstance(value, dict):
                    list_values.append(value)
                else:
                    pass
            self._java_obj.setCategoricalFeaturesPy(list_values)
        return self

    def setNumericFeatures(self, values: List[Union[str, Dict]]):
        """
        Args:
        values: The list of values that represent numeric features to explain.
        Values are list of dicts with parameters or just a list of names of numeric features
        """
        if len(values) == 0:
            pass
        else:
            list_values = []
            for value in values:
                if isinstance(value, str):
                    list_values.append({"name": value})
                elif isinstance(value, dict):
                    list_values.append(value)
                else:
                    pass
            self._java_obj.setNumericFeaturesPy(list_values)
        return self
