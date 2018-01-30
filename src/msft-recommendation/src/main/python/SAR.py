# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.


import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from mmlspark.Utils import *
from mmlspark._SAR import _SAR as sar
from mmlspark._SAR import SARModel as model


class SAR(sar):
    def itemFeatures(self, userColumn, itemColumn, supportThreshold, transformedDf):
        return self._call_java("itemFeatures", userColumn, itemColumn, supportThreshold, transformedDf)


class SARModel(model):
    def recommendForAllUsers(self, numItems):
        return self._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numItems):
        return self._call_java("recommendForAllItems", numItems)
