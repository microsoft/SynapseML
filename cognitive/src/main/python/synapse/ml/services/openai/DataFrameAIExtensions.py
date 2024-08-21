# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
import os, json, subprocess, unittest

if sys.version >= "3":
    basestring = str

import pyspark
from pyspark import SparkContext
from pyspark import sql
from pyspark.ml.param.shared import *
from pyspark.rdd import RDD

from pyspark.sql import SparkSession, SQLContext

from synapse.ml.core.init_spark import *
spark = init_spark()
sc = SQLContext(spark.sparkContext)

class AIFunctions:
    def __init__(self, df):
        self.df = df
        self.subscriptionKey = None
        self.deploymentName = None
        self.customServiceName = None

    def setup(self, subscriptionKey = None, deploymentName = None, customServiceName = None):
        self.subscriptionKey = subscriptionKey
        self.deploymentName = deploymentName
        self.customServiceName = customServiceName

    def gen(self, template, outputCol = None, **options):
        jvm = SparkContext.getOrCreate()._jvm
        prompt = jvm.com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt()
        prompt = prompt.setSubscriptionKey(self.subscriptionKey)
        prompt = prompt.setDeploymentName(self.deploymentName)
        prompt = prompt.setCustomServiceName(self.customServiceName)
        prompt = prompt.setOutputCol(outputCol)
        prompt = prompt.setPromptTemplate(template)
        results = prompt.transform(self.df._jdf)
        results.createOrReplaceTempView("my_temp_view")
        results = spark.sql("SELECT * FROM my_temp_view")
        return results

def get_AI_functions(df):
    if not hasattr(df, "_ai_instance"):
        df._ai_instance = AIFunctions(df)
    return df._ai_instance

setattr(pyspark.sql.DataFrame, "ai", property(get_AI_functions))
