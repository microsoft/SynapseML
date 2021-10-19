# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
from pyspark import SQLContext
from pyspark import SparkContext

if sys.version >= '3':
    basestring = str

from synapse.ml.automl._TuneHyperparametersModel import _TuneHyperparametersModel
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import inherit_doc

@inherit_doc
class TuneHyperparametersModel(_TuneHyperparametersModel):
    def getBestModel(self):
        """
        Returns the best model.
        """
        return JavaParams._from_java(self._java_obj.getBestModel())

    def getBestModelInfo(self):
        """
        Returns the best model parameter info.
        """
        return self._java_obj.getBestModelInfo()
