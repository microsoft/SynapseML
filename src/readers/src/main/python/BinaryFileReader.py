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
from pyspark.sql.types import *

BinaryFileFields = ["path", "bytes"]

BinaryFileSchema = StructType([
    StructField(BinaryFileFields[0], StringType(),  True),
    StructField(BinaryFileFields[1], BinaryType(), True) ])

def readBinaryFiles(self, path, recursive = False, sampleRatio = 1.0, inspectZip = True):
    """
    Reads the directory of binary files from the local or remote (WASB) source

    :param str path: Path to the file directory
    :param bool recursive: Recursive search flag
    :param double sampleRatio: Fraction of the files loaded into the dataframe
    :return: DataFrame with a single column "value"; see binaryFileSchema for details
    :rtype: DataFrame
    """
    ctx = SparkContext.getOrCreate()
    reader = ctx._jvm.com.microsoft.ml.spark.BinaryFileReader
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    jsession = sql_ctx.sparkSession._jsparkSession
    jresult = reader.read(path, recursive, jsession, float(sampleRatio), inspectZip)
    return DataFrame(jresult, sql_ctx)

setattr(sql.SparkSession, 'readBinaryFiles', classmethod(readBinaryFiles))

def isBinaryFile(df, column):
    """
    Returns True if the column contains binary files

    :param DataFrame df: The DataFrame to be processed
    :param bool column: The name of the column being inspected
    :return: True if the colum is a binary files column
    :rtype: bool
    """
    ctx = SparkContext.getOrCreate()
    schema = ctx._jvm.com.microsoft.ml.spark.schema.BinaryFileSchema
    return schema.isBinaryFile(df._jdf, column)
