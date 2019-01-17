# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

import pyspark
from pyspark import SparkContext
from pyspark import sql
from pyspark.ml.param.shared import *
from pyspark.sql import DataFrame


def streamToAzureSearch(df, url, options=dict()):
    jvm = SparkContext.getOrCreate()._jvm
    writer = jvm.com.microsoft.ml.spark.AzureSearchWriter
    return writer.stream(df._jdf, url, options)

setattr(pyspark.sql.DataFrame, 'streamToAzureSearch', streamToAzureSearch)

def writeToAzureSearch(df, url, options=dict()):
    jvm = SparkContext.getOrCreate()._jvm
    writer = jvm.com.microsoft.ml.spark.AzureSearchWriter
    writer.write(df._jdf, url, options)

setattr(pyspark.sql.DataFrame, 'writeToAzureSearch', writeToAzureSearch)
