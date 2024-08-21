# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest
import os, json, subprocess, unittest

from synapse.ml.io.http import *
from pyspark.sql.types import *
from synapse.ml.services.openai import *

from pyspark.sql import SparkSession, SQLContext

from synapse.ml.core.init_spark import *
spark = init_spark()
sc = SQLContext(spark.sparkContext)

class DataFrameAIExtentionsTest(unittest.TestCase):
    def test_gen(self):
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

        secretJson = subprocess.check_output(
            "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key-2",
            shell=True,
        )
        openai_api_key = json.loads(secretJson)["value"]
        print(openai_api_key)

        df.ai.setup(subscriptionKey=openai_api_key, deploymentName="gpt-35-turbo", customServiceName="synapseml-openai-2")

        results = df.ai.gen("Complete this comma separated list of 5 {category}: {text}, ", outputCol="outParsed")
        results.select("outParsed").show(truncate = False)
        nonNullCount = results.filter(col("outParsed").isNotNull()).count()
        assert (nonNullCount == 3)

    def test_gen_2(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("address", StringType(), True)
        ])

        data = [
            ("Anne F.", "123 First Street, 98053"),
            ("George K.", "345 Washington Avenue, London"),
        ]

        df = spark.createDataFrame(data, schema)

        secretJson = subprocess.check_output(
            "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key-2",
            shell=True,
        )
        openai_api_key = json.loads(secretJson)["value"]

        df.ai.setup(subscriptionKey=openai_api_key, deploymentName="gpt-35-turbo", customServiceName="synapseml-openai-2")
        results = df.ai.gen("Generate the likely country of {name}, given that they are from {address}. It is imperitive that your response contains the country only, no elaborations.", outputCol="outParsed")
        results.select("outParsed").show(truncate = False)
        nonNullCount = results.filter(col("outParsed").isNotNull()).count()
        assert (nonNullCount == 2)

if __name__ == "__main__":
    result = unittest.main()
