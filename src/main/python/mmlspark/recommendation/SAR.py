# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.


import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc
from mmlspark.core.schema.Utils import *
from mmlspark.recommendation._SAR import _SAR as sar
from mmlspark.recommendation.SARModel import SARModel as sarm


@inherit_doc
class SAR(sar):
    def _create_model(self, java_model):
        model = sarm()
        model._java_obj = java_model
        model._transfer_params_from_java()
        return model
