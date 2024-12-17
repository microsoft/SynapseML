# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults
from synapse.ml.services.openai.OpenAIPrompt import OpenAIPrompt
import unittest
from pyspark.sql import SQLContext

from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class TestOpenAIDefaults(unittest.TestCase):
    def test_setters_and_getters(self):
        defaults = OpenAIDefaults()

        defaults.set_deployment_name("Bing Bong")
        defaults.set_subscription_key("SubKey")
        defaults.set_temperature(0.05)

        self.assertEqual(defaults.get_deployment_name(), "Bing Bong")
        self.assertEqual(defaults.get_subscription_key(), "SubKey")
        self.assertEqual(defaults.get_temperature(), 0.05)

    def test_resetters(self):
        defaults = OpenAIDefaults()

        defaults.set_deployment_name("Bing Bong")
        defaults.set_subscription_key("SubKey")
        defaults.set_temperature(0.05)

        self.assertEqual(defaults.get_deployment_name(), "Bing Bong")
        self.assertEqual(defaults.get_subscription_key(), "SubKey")
        self.assertEqual(defaults.get_temperature(), 0.05)

        defaults.reset_deployment_name()
        defaults.reset_subscription_key()
        defaults.reset_temperature()

        self.assertEqual(defaults.get_deployment_name(), None)
        self.assertEqual(defaults.get_subscription_key(), None)
        self.assertEqual(defaults.get_temperature(), None)

    def test_two_defaults(self):
        defaults = OpenAIDefaults()

        defaults.set_deployment_name("Bing Bong")
        self.assertEqual(defaults.get_deployment_name(), "Bing Bong")

        defaults2 = OpenAIDefaults()
        defaults.set_deployment_name("Bing Bong")
        defaults2.set_deployment_name("Vamos")
        self.assertEqual(defaults.get_deployment_name(), "Vamos")

        defaults2.set_deployment_name("Test 2")
        defaults.set_deployment_name("Test 1")
        self.assertEqual(defaults.get_deployment_name(), "Test 1")


if __name__ == "__main__":
    result = unittest.main()
