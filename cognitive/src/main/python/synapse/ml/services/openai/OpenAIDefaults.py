# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

import pyspark
from pyspark import SparkContext


def getOption(opt):
    if opt.isDefined():
        return opt.get()
    else:
        return None


class OpenAIDefaults:
    def __init__(self):
        self.defaults = (
            SparkContext.getOrCreate()._jvm.com.microsoft.azure.synapse.ml.services.openai.OpenAIDefaults
        )

    def set_deployment_name(self, name):
        self.defaults.setDeploymentName(name)

    def get_deployment_name(self):
        return getOption(self.defaults.getDeploymentName())

    def reset_deployment_name(self):
        self.defaults.resetDeploymentName()

    def set_subscription_key(self, key):
        self.defaults.setSubscriptionKey(key)

    def get_subscription_key(self):
        return getOption(self.defaults.getSubscriptionKey())

    def reset_subscription_key(self):
        self.defaults.resetSubscriptionKey()

    def set_temperature(self, temp):
        self.defaults.setTemperature(float(temp))

    def get_temperature(self):
        return getOption(self.defaults.getTemperature())

    def reset_temperature(self):
        self.defaults.resetTemperature()

    def set_URL(self, URL):
        self.defaults.setURL(URL)

    def get_URL(self):
        return getOption(self.defaults.getURL())

    def reset_URL(self):
        self.defaults.resetURL()
