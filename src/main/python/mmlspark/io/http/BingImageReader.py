# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

import pyspark
from pyspark import SparkContext
from pyspark import sql
from pyspark.sql import DataFrame
from pyspark.sql.types import *

def streamBingImages(self, searchTerms, key, url, batchSize = 10, imgsPerBatch = 10):
    ctx = SparkContext.getOrCreate()
    reader = ctx._jvm.org.apache.spark.sql.execution.streaming.BingImageSource
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    jsession = sql_ctx.sparkSession._jsparkSession
    jdf = reader(searchTerms, key, url, batchSize, imgsPerBatch).load(jsession)
    return DataFrame(jdf, sql_ctx)

setattr(sql.SparkSession, 'streamBingImages', classmethod(streamBingImages))
