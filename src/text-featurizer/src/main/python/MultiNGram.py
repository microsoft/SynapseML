# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark._MultiNGram import _MultiNGram
from pyspark.ml.common import inherit_doc

@inherit_doc
class MultiNGram(_MultiNGram):

    def setRange(self, min, max):
        self._java_obj = self._java_obj.setRange(min, max)
        return self

    def getRange(self):
        return self._java_obj.getRange()
