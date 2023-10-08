# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.causal._DiffInDiffModel import _DiffInDiffModel
from pyspark.ml.common import inherit_doc
import numpy as np

@inherit_doc
class DiffInDiffModel(_DiffInDiffModel):
    pass
