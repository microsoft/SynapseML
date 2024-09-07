# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.


import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.core.schema.Utils import *
from synapse.ml.recommendation._SARModel import _SARModel


@inherit_doc
class SARModel(_SARModel):
    def recommendForAllUsers(self, numItems):
        return self._call_java("recommendForAllUsers", numItems)
