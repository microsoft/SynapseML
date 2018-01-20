# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.


import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from mmlspark.Utils import *
from mmlspark._MsftRecommendation import MsftRecommendationModel as model


class MsftRecommendationModel(model):
    def recommendForAllUsers(self, numItems):
        return self._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numItems):
        return self._call_java("recommendForAllItems", numItems)
