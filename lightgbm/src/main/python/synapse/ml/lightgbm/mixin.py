# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.ml.common import inherit_doc
from synapse.ml.core.serialize.java_params_patch import *

@inherit_doc
class LightGBMModelMixin:
    def saveNativeModel(self, filename, overwrite=True):
        """
        Save the booster as string format to a local or WASB remote location.
        """
        self._java_obj.saveNativeModel(filename, overwrite)

    def getFeatureImportances(self, importance_type="split"):
        """
        Get the feature importances as a list.  The importance_type can be "split" or "gain".
        """
        return list(self._java_obj.getFeatureImportances(importance_type))

    def getFeatureShaps(self, vector):
        """
        Get the local shap feature importances.
        """
        if isinstance(vector, DenseVector):
            dense_values = [float(v) for v in vector]
            return list(self._java_obj.getDenseFeatureShaps(dense_values))
        elif isinstance(vector, SparseVector):
            sparse_indices = [int(i) for i in vector.indices]
            sparse_values = [float(v) for v in vector.values]
            return list(self._java_obj.getSparseFeatureShaps(vector.size, sparse_indices, sparse_values))
        else:
            raise TypeError("Vector argument to getFeatureShaps must be a pyspark.linalg sparse or dense vector type")

    def getBoosterBestIteration(self):
        """Get the best iteration from the booster.

        Returns:
            The best iteration, if early stopping was triggered.
        """
        return self._java_obj.getBoosterBestIteration()

    def getBoosterNumTotalIterations(self):
        """Get the total number of iterations trained.

        Returns:
            The total number of iterations trained.
        """
        return self._java_obj.getBoosterNumTotalIterations()

    def getBoosterNumTotalModel(self):
        """Get the total number of models trained.

        Returns:
            The total number of models.
        """
        return self._java_obj.getBoosterNumTotalModel()

    def getBoosterNumFeatures(self):
        """Get the number of features from the booster.

        Returns:
            The number of features.
        """
        return self._java_obj.getBoosterNumFeatures()

    def setPredictDisableShapeCheck(self, value=None):
        """
        Set shape check or not when predict.
        """
        if not value:
            value = False
        else:
            value = True

        self._java_obj.setPredictDisableShapeCheck(value)
