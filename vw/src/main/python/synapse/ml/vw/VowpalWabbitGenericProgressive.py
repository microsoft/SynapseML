# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitGenericProgressive import (
    _VowpalWabbitGenericProgressive,
)
from synapse.ml.vw.VowpalWabbitPythonBase import (
    VowpalWabbitPythonBase,
)
from pyspark.ml.common import inherit_doc


@inherit_doc
class VowpalWabbitGenericProgressive(
    _VowpalWabbitGenericProgressive, VowpalWabbitPythonBase
):
    pass
