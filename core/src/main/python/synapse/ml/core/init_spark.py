# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.core import __spark_package_version__


def init_spark():
    from pyspark.sql import SparkSession, SQLContext

    return (
        SparkSession.builder.master("local[*]")
        .appName("PysparkTests")
        .config(
            "spark.jars.packages",
            "com.microsoft.azure:synapseml_2.12:"
            + __spark_package_version__
            + ",org.apache.spark:spark-avro_2.12:3.4.1",
        )
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.shuffle.partitions", 10)
        .config("spark.sql.crossJoin.enabled", "true")
        .getOrCreate()
    )
