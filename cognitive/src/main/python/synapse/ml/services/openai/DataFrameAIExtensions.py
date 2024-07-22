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


def prompt(df, template, **options):
    jvm = SparkContext.getOrCreate()._jvm

    secretJson = subprocess.check_output(
        "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key-2",
        shell=True,
    )
    openai_api_key = json.loads(secretJson)["value"]

    prompt = jvm.com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt
    prompt = prompt().setSubscriptionKey(openai_api_key)
    prompt = prompt.setDeploymentName("gpt-35-turbo")
    prompt = prompt.setOutputCol("outParsed")
    prompt = prompt.setTemperature(0)
    prompt = prompt.setPromptTemplate(template)
    print(prompt.transform(df._jdf))
    return prompt.transform(df._jdf)


setattr(pyspark.sql.DataFrame, "prompt", prompt)
