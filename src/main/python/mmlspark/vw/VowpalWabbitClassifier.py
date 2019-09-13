# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
from pyspark import SQLContext
from pyspark import SparkContext

if sys.version >= '3':
    basestring = str

from mmlspark.vw._VowpalWabbitClassifier import _VowpalWabbitClassificationModel
from pyspark.ml.common import inherit_doc
from pyspark.ml.wrapper import JavaParams
from mmlspark.core.serialize.java_params_patch import *

@inherit_doc
class VowpalWabbitClassificationModel(_VowpalWabbitClassificationModel):
    def saveNativeModel(self, filename):
        """
        Save the native model to a local or WASB remote location.
        """
        self._java_obj.saveNativeModel(filename)