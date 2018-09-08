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

    def setQuery(self, value):
        self._java_obj = self._java_obj.setQuery(value)
        return self

    def setQueryCol(self, value):
        self._java_obj = self._java_obj.setQueryCol(value)
        return self

    def setMarket(self, value):
        self._java_obj = self._java_obj.setMarket(value)
        return self

    def setMarketCol(self, value):
        self._java_obj = self._java_obj.setMarketCol(value)
        return self

    @staticmethod
    def getUrlTransformer(cls, imageCol, urlCol):
        from mmlspark import BingImageSearch
        from pyspark.sql import SparkSession
        bis = SparkSession.builder.getOrCreate()._jvm.com.microsoft.ml.spark.BingImageSearch
        return Lambda._from_java(bis.getUrlTransformer(imageCol,urlCol))

    @staticmethod
    def downloadFromUrls(cls, pathCol, bytesCol, concurrency, timeout):
        from mmlspark import BingImageSearch
        from pyspark.sql import SparkSession
        bis = SparkSession.builder.getOrCreate()._jvm.com.microsoft.ml.spark.BingImageSearch
        return Lambda._from_java(bis.downloadFromUrls(pathCol, bytesCol, concurrency, timeout))
