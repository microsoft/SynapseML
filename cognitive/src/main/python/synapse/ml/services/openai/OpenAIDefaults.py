# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

import pyspark
from pyspark import SparkContext

class OpenAIDefaults:
    def __init__(self):
        self.defaults = SparkContext.getOrCreate()._jvm.com.microsoft.azure.synapse.ml.services.openai.OpenAIDefaults

    def set_deployment_name(self, name):
        self.defaults.setDeploymentName(name)

    def set_subscription_key(self, key):
        self.defaults.setSubscriptionKey(key)

    def set_temperature(self, temp):
        self.defaults.setTemperature(temp)

    def get_deployment_name(self):
        self.defaults.getDeploymentName()

    def get_subscription_key(self):
        self.defaults.getSubscriptionKey()

    def get_temperature(self):
        self.defaults.getTemperature()
