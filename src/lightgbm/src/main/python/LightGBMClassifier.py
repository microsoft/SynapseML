# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
from pyspark import SQLContext
from pyspark import SparkContext

if sys.version >= '3':
    basestring = str

from mmlspark._LightGBMClassifier import _LightGBMClassifier
from mmlspark._LightGBMClassifier import M
from pyspark.ml.common import inherit_doc

@inherit_doc
class LightGBMClassifier(_LightGBMClassifier):
    def _create_model(self, java_model):
        model = LightGBMClassificationModel()
        model._java_obj = java_model
        model._transfer_params_from_java()
        return model

@inherit_doc
class LightGBMClassificationModel(M):
    def saveNativeModel(self, sparkSession, filename):
        """
        Save the booster as string format to a local or WASB remote location.
        """
        ctx = SparkContext.getOrCreate()
        sql_ctx = SQLContext.getOrCreate(ctx)
        jsession = sql_ctx.sparkSession._jsparkSession
        self._java_obj.saveNativeModel(jsession, filename)

    def getFeatureImportances(self, importance_type="split"):
        """
        Get the feature importances.  The importance_type can be "split" or "gain".
        """
        self._java_obj.getFeatureImportances(importance_type)
