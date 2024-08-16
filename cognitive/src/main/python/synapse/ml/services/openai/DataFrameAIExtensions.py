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
from pyspark.sql import DataFrame

class AIFunctions:
    def __init__(self, df):
        self.df = df

    def gen(self, template, **options):
        jvm = SparkContext.getOrCreate()._jvm

        secretJson = subprocess.check_output(
            "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key-2",
            shell=True,
        )
        openai_api_key = json.loads(secretJson)["value"]

        prompt = jvm.com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt()
        prompt = prompt.setSubscriptionKey(openai_api_key)
        prompt = prompt.setDeploymentName("gpt-35-turbo")
        prompt = prompt.setCustomServiceName("synapseml-openai-2")
        prompt = prompt.setOutputCol("outParsed")
        prompt = prompt.setPromptTemplate(template)
        results = prompt.transform(self.df._jdf)
        return DataFrame(results, self.df.sql_ctx)

def get_AI_functions(df):
    return AIFunctions(df)

setattr(pyspark.sql.DataFrame, "ai", property(get_AI_functions))
