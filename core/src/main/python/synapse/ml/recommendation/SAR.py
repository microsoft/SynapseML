# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.core.schema.Utils import *
from synapse.ml.recommendation._SAR import _SAR

@inherit_doc
class SAR(_SAR):
    def __init__(self, **kwargs):
        _SAR.__init__(self, **kwargs)

    def calculateUserItemAffinities(self, dataset):
        if dataset.schema[self.getUserCol()].dataType == StringType():
            dataset = dataset.withColumn(self.getUserCol(), dataset[self.getUserCol()].cast("int"))
        if dataset.schema[self.getItemCol()].dataType == StringType():
            dataset = dataset.withColumn(self.getItemCol(), dataset[self.getItemCol()].cast("int"))
        return self._call_java("calculateUserItemAffinities", dataset)

    def calculateItemItemSimilarity(self, dataset):
        if dataset.schema[self.getUserCol()].dataType == StringType():
            dataset = dataset.withColumn(self.getUserCol(), dataset[self.getUserCol()].cast("int"))
        if dataset.schema[self.getItemCol()].dataType == StringType():
            dataset = dataset.withColumn(self.getItemCol(), dataset[self.getItemCol()].cast("int"))
        return self._call_java("calculateItemItemSimilarity", dataset)
