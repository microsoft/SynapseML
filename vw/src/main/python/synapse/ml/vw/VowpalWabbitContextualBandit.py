# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitContextualBandit import _VowpalWabbitContextualBandit
from synapse.ml.vw.VowpalWabbitPythonBase import (
    VowpalWabbitPythonBase,
)
from pyspark.ml.common import inherit_doc
from pyspark import SparkContext
from pyspark.ml.wrapper import JavaWrapper
from pyspark.ml.common import _py2java


@inherit_doc
class VowpalWabbitContextualBandit(
    _VowpalWabbitContextualBandit, VowpalWabbitPythonBase
):
    pass
