# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitFeaturizer import _VowpalWabbitFeaturizer
from pyspark.ml.common import inherit_doc


@inherit_doc
class VowpalWabbitFeaturizer(_VowpalWabbitFeaturizer):
    pass
