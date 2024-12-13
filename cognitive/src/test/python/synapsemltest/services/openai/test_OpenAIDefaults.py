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
    def test_OpenAIDefaults(self):
        defaults = OpenAIDefaults()

        defaults.set_deployment_name("Bing Bong")
        defaults.set_subscription_key("SubKey")
        defaults.set_temperature(0.05)

        self.assertEqual(defaults.get_deployment_name(), "Bing Bong")
        self.assertEqual(defaults.get_subscription_key(), "SubKey")
        self.assertEqual(defaults.get_temperature(), 0.05)

if __name__ == "__main__":
    result = unittest.main()
