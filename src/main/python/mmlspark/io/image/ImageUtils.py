# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

import pyspark
from pyspark import SparkContext
from pyspark import sql
from pyspark.ml.param.shared import *
from pyspark.sql import DataFrame


def readFromPaths(df, pathCol, imageCol="image"):
    """
    Reads images from a column of filenames

    Args:
        df (DataFrame): The DataFrame to be processed
        pathCol  (str): The name of the column containing filenames
        imageCol (str): The name of the added column of images

    Returns:
        df: The dataframe with loaded images
    """
    ctx = SparkContext.getOrCreate()
    jvm = ctx.getOrCreate()._jvm
    reader = jvm.com.microsoft.ml.spark.io.image.ImageUtils
    jresult = reader.readFromPaths(df._jdf, pathCol, imageCol)
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    return DataFrame(jresult, sql_ctx)


def readFromStrings(df, bytesCol, imageCol="image", dropPrefix=False):
    """
    Reads images from a column of filenames

    Args:
        df (DataFrame): The DataFrame to be processed
        pathCol  (str): The name of the column containing filenames
        imageCol (str): The name of the added column of images

    Returns:
        df: The dataframe with loaded images
    """
    ctx = SparkContext.getOrCreate()
    jvm = ctx.getOrCreate()._jvm
    reader = jvm.com.microsoft.ml.spark.io.image.ImageUtils
    jresult = reader.readFromStrings(df._jdf, bytesCol, imageCol, dropPrefix)
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    return DataFrame(jresult, sql_ctx)

