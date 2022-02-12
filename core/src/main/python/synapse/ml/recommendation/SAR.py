# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= "3":
    basestring = str

from pyspark.ml.tuning import _ValidatorParams
from pyspark.ml.wrapper import JavaParams
from pysarplus import SARPlus
from synapse.ml.recommendation._SAR import _SAR


class RankingTrainValidationSplit(_ValidatorParams, _SAR):
    def __init__(self, **kwargs):
        _SAR.__init__(self, **kwargs)

    def _to_java(self):
        estimator, epms, evaluator = _ValidatorParams._to_java_impl(self)

        _java_obj = JavaParams._new_java_obj(
            "com.microsoft.azure.synapse.ml.recommendation.SAR", self.uid
        )
        _java_obj.setItemCol(self.getItemCol())
        _java_obj.setUserCol(self.getUserCol())
        _java_obj.setRatingCol(self.getRatingCol())
        _java_obj.setTimeCol(self.getTimeCol())

        return _java_obj

    def _fit(self, dataset):
        sarplus = SARPlus(
          col_user=self.getUserCol(),
          col_item=self.getItemCol(),
          col_rating=self.getRatingCol(),
          col_timestamp=self.getTimeCol()
        )
        sarplus.fit(dataset)
        
        return sarplus
