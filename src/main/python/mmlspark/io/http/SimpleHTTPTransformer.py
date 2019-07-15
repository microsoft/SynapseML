# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark.io.http._SimpleHTTPTransformer import _SimpleHTTPTransformer
from pyspark.ml.common import inherit_doc

@inherit_doc
class SimpleHTTPTransformer(_SimpleHTTPTransformer):

    def setUrl(self, value):
        return self.setInputParser(self.getInputParser().setUrl(value))
