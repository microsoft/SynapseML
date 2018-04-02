# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark._TextFeaturizer import _TextFeaturizer
from pyspark.ml.common import inherit_doc

@inherit_doc
class TextFeaturizer(_TextFeaturizer):

    def setNGramRange(self, min, max):
        self._java_obj = self._java_obj.setNGramRange(min, max)
        return self

    def getNGramRange(self):
        return self._java_obj.getNGramRange()

    def setNGramLength(self, value):
        self._java_obj = self._java_obj.setNGramLength(value)
        return self

    def getNGramLength(self):
        return self._java_obj.getNGramLength()

