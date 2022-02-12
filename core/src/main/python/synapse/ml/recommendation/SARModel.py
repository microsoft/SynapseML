# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.


import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.core.schema.Utils import *
from pysarplus import SARPlus


@inherit_doc
class SARModel:
    
    def __init__(self, sarplus: SARPlus=None):
        self.sarplus = sarplus

    def recommendForAllUsers(self, numItems):
        return self.sarplus.recommend_k_items(top_k=numItems)
