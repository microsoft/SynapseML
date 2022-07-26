# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitMulticlassClassifier import _VowpalWabbitMulticlassClassifier
from synapse.ml.vw.VowpalWabbitPythonBase import (
    VowpalWabbitPythonBase,
)
from pyspark.ml.common import inherit_doc


@inherit_doc
class VowpalWabbitMulticlassClassifier(_VowpalWabbitMulticlassClassifier, VowpalWabbitPythonBase):
    pass