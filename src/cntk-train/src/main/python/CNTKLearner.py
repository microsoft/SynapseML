# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark._CNTKLearner import _CNTKLearner
from mmlspark.CNTKModel import CNTKModel as CNTKmod
from pyspark.ml.common import inherit_doc

@inherit_doc
class CNTKLearner(_CNTKLearner):
    """
    Create CNTK model from existing java model
    :param py4j.java_gateway.JavaObject java_model: see Scala CNTKModel documentation
    """
    def _create_model(self, java_model):
        model = CNTKmod()
        model._java_obj = java_model
        model._transfer_params_from_java()
        return model
