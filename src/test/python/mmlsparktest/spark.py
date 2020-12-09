# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.sql import SparkSession, SQLContext
import os
import mmlspark

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PysparkTests") \
    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:" + mmlspark.__spark_package_version__) \
    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.sql.shuffle.partitions", 10) \
    .config("spark.sql.crossJoin.enabled", "true") \
    .getOrCreate()

sc = SQLContext(spark.sparkContext)
