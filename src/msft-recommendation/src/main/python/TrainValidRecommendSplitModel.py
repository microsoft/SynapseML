# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc
from pyspark.ml.util import *
from mmlspark._TrainValidRecommendSplitModel import _TrainValidRecommendSplitModel as tvModel
from mmlspark.Utils import *
from mmlspark.TrainTestSplit import *


@inherit_doc
class TrainValidRecommendSplitModel(tvModel):
    def recommendForAllUsers(self, numItems):
        return self._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numItems):
        return self._call_java("recommendForAllItems", numItems)
    #
    # def _transform(self, dataset):
    #     return self.bestModel.transform(dataset)
