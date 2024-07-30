# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

from synapse.ml.io.http import *
from pyspark.sql.types import *
from synapse.ml.services.openai import *

from pyspark.sql import SparkSession, SQLContext

from synapse.ml.core.init_spark import *
spark = init_spark()
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
