# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitRegressor import _VowpalWabbitRegressor
from synapse.ml.vw.VowpalWabbitPythonBase import (
    VowpalWabbitPythonBase,
)
from pyspark.ml.common import inherit_doc


@inherit_doc
class VowpalWabbitRegressor(_VowpalWabbitRegressor, VowpalWabbitPythonBase):
    pass
