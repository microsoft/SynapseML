# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark._BingImageSearch import _BingImageSearch
from mmlspark.Lambda import Lambda
from pyspark.ml.common import inherit_doc

@inherit_doc
class BingImageSearch(_BingImageSearch):

    def setMiniBatchSize(self, n):
        self._java_obj = self._java_obj.setMiniBatchSize(n)
        return self

    def setOffsetCol(self, v):
        self._java_obj = self._java_obj.setOffsetCol(v)
        return self

    def setOffset(self, v):
        self._java_obj = self._java_obj.setOffset(v)
        return self

    def setQuery(self, v):
        self._java_obj = self._java_obj.setQuery(v)
        return self

    def setQueryCol(self, v):
        self._java_obj = self._java_obj.setQueryCol(v)
        return self

    def setCount(self, v):
        self._java_obj = self._java_obj.setCount(v)
        return self

    def setCountCol(self, v):
        self._java_obj = self._java_obj.setCountCol(v)
        return self

    def setImageType(self, v):
        self._java_obj = self._java_obj.setImageType(v)
        return self

    def setImageTypeCol(self, v):
        self._java_obj = self._java_obj.setImageTypeCol(v)
        return self

    @staticmethod
    def getUrlTransformer(cls, imageCol, urlCol):
        from mmlspark import BingImageSearch
        from pyspark.sql import SparkSession
        bis = SparkSession.builder.getOrCreate()._jvm.com.microsoft.ml.spark.BingImageSearch
        return Lambda._from_java(bis.getUrlTransformer(imageCol,urlCol))
