# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.recommendation._RankingTrainValidationSplitModel import _RankingTrainValidationSplitModel
from pyspark.ml.util import *


# Load information from java_stage to the instance.
@inherit_doc
class RankingTrainValidationSplitModel(_RankingTrainValidationSplitModel):

    def recommendForAllUsers(self, numItems):
        return self._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numUsers):
        return self._call_java("recommendForAllItems", numUsers)
