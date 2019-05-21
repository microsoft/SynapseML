# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark.cognitive._BingImageSearch import _BingImageSearch
from mmlspark.stages import Lambda
from pyspark.ml.common import inherit_doc
from pyspark.sql import SparkSession

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
    def getUrlTransformer(imageCol, urlCol):
        bis = SparkSession.builder.getOrCreate()._jvm.com.microsoft.ml.spark.cognitive.BingImageSearch
        return Lambda._from_java(bis.getUrlTransformer(imageCol,urlCol))

    @staticmethod
    def downloadFromUrls(pathCol, bytesCol, concurrency, timeout):
        bis = SparkSession.builder.getOrCreate()._jvm.com.microsoft.ml.spark.cognitive.BingImageSearch
        return Lambda._from_java(bis.downloadFromUrls(pathCol, bytesCol, concurrency, timeout))
