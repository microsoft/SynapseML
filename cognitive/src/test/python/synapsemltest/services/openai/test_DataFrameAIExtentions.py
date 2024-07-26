# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

from synapse.ml.io.http import *
from pyspark.sql.functions import struct
from pyspark.sql.types import *
from synapse.ml.services.openai import *

from pyspark.sql import SparkSession, SQLContext
from synapse.ml.core import __spark_package_version__
spark = (SparkSession.builder
         .master("local[*]")
         .appName("PysparkTests")
         .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:" + __spark_package_version__ + ",org.apache.spark:spark-avro_2.12:3.4.1")
         .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
         .config("spark.executor.heartbeatInterval", "60s")
         .config("spark.sql.shuffle.partitions", 10)
         .config("spark.sql.crossJoin.enabled", "true")
         .getOrCreate())

sc = SQLContext(spark.sparkContext)

class DataFrameAIExtentionsTest(unittest.TestCase):
    def test_prompt(self):
        schema = StructType([
            StructField("text", StringType(), True),
            StructField("category", StringType(), True)
        ])

        data = [
            ("apple", "fruits"),
            ("mercedes", "cars"),
            ("cake", "dishes"),
        ]

        df = spark.createDataFrame(data, schema)

        results = df.ai.prompt("here is a comma separated list of 5 {category}: {text}, ")
        results.show()

    def test_prompt_2(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("address", StringType(), True)
        ])

        data = [
            ("Anne F.", "123 First Street, 98053"),
            ("George K.", "345 Washington Avenue, London"),
        ]

        df = spark.createDataFrame(data, schema)

        results = df.ai.prompt("Generate the likely country of {name}, given that they are from {address}. It is imperitive that your response contains the country only, no elaborations.")
        results.show()


if __name__ == "__main__":
    result = unittest.main()
