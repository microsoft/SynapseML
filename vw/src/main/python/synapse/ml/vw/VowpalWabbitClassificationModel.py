# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitClassificationModel import (
    _VowpalWabbitClassificationModel,
)
from synapse.ml.vw.VowpalWabbitPythonBase import (
    VowpalWabbitPythonBaseModel,
)
from pyspark.ml.common import inherit_doc
from pyspark import SparkContext, SQLContext
from pyspark.sql import DataFrame


@inherit_doc
class VowpalWabbitClassificationModel(
    _VowpalWabbitClassificationModel, VowpalWabbitPythonBaseModel
):
    pass
