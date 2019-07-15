# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

import pyspark

def _mlTransform(self, t):
    return t.transform(self)

setattr(pyspark.sql.dataframe.DataFrame, 'mlTransform', _mlTransform)

def _mlFit(self, e):
    return e.fit(self)

setattr(pyspark.sql.dataframe.DataFrame, 'mlFit', _mlFit)
