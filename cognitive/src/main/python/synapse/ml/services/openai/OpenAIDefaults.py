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
        temp_float = float(temp)
        if not (0.0 <= temp_float <= 2.0):
            raise ValueError(
                f"Temperature must be between 0.0 and 2.0, got: {temp_float}"
            )
        self.defaults.setTemperature(temp_float)

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

    def set_seed(self, seed):
        self.defaults.setSeed(int(seed))

    def get_seed(self):
        return getOption(self.defaults.getSeed())

    def reset_seed(self):
        self.defaults.resetSeed()

    def set_top_p(self, top_p):
        top_p_float = float(top_p)
        if not (0.0 <= top_p_float <= 1.0):
            raise ValueError(f"TopP must be between 0.0 and 1.0, got: {top_p_float}")
        self.defaults.setTopP(top_p_float)

    def get_top_p(self):
        return getOption(self.defaults.getTopP())

    def reset_top_p(self):
        self.defaults.resetTopP()

    def set_api_version(self, api_version):
        self.defaults.setApiVersion(api_version)

    def get_api_version(self):
        return getOption(self.defaults.getApiVersion())

    def reset_api_version(self):
        self.defaults.resetApiVersion()

    def set_model(self, ai_foundry_model):
        self.defaults.setModel(ai_foundry_model)

    def get_model(self):
        return getOption(self.defaults.getModel())

    def reset_model(self):
        self.defaults.resetModel()
