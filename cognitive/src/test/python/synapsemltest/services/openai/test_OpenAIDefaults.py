# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest
import json
import subprocess

from pyspark.sql import SQLContext
from pyspark.sql.functions import col

from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults
from synapse.ml.services.openai.OpenAIPrompt import OpenAIPrompt
from synapse.ml.services.openai.OpenAIChatCompletion import OpenAIChatCompletion
from synapse.ml.core.init_spark import init_spark

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class TestOpenAIDefaults(unittest.TestCase):
    def test_setters_and_getters(self):
        defaults = OpenAIDefaults()

        defaults.set_deployment_name("Bing Bong")
        defaults.set_subscription_key("SubKey")
        defaults.set_temperature(0.05)
        defaults.set_seed(42)
        defaults.set_top_p(0.9)
        defaults.set_URL("Test URL/")
        defaults.set_api_version("2024-05-01-preview")
        defaults.set_model("grok-3-mini")
        defaults.set_embedding_deployment_name("text-embedding-ada-002")

        self.assertEqual(defaults.get_deployment_name(), "Bing Bong")
        self.assertEqual(defaults.get_subscription_key(), "SubKey")
        self.assertEqual(defaults.get_temperature(), 0.05)
        self.assertEqual(defaults.get_seed(), 42)
        self.assertEqual(defaults.get_top_p(), 0.9)
        self.assertEqual(defaults.get_URL(), "Test URL/")
        self.assertEqual(defaults.get_api_version(), "2024-05-01-preview")
        self.assertEqual(defaults.get_model(), "grok-3-mini")
        self.assertEqual(
            defaults.get_embedding_deployment_name(), "text-embedding-ada-002"
        )

    def test_resetters(self):
        defaults = OpenAIDefaults()

        defaults.set_deployment_name("Bing Bong")
        defaults.set_subscription_key("SubKey")
        defaults.set_temperature(0.05)
        defaults.set_seed(42)
        defaults.set_top_p(0.9)
        defaults.set_URL("Test URL/")
        defaults.set_api_version("2024-05-01-preview")
        defaults.set_model("grok-3-mini")
        defaults.set_embedding_deployment_name("text-embedding-ada-002")

        self.assertEqual(defaults.get_deployment_name(), "Bing Bong")
        self.assertEqual(defaults.get_subscription_key(), "SubKey")
        self.assertEqual(defaults.get_temperature(), 0.05)
        self.assertEqual(defaults.get_seed(), 42)
        self.assertEqual(defaults.get_top_p(), 0.9)
        self.assertEqual(defaults.get_URL(), "Test URL/")
        self.assertEqual(defaults.get_api_version(), "2024-05-01-preview")
        self.assertEqual(defaults.get_model(), "grok-3-mini")
        self.assertEqual(
            defaults.get_embedding_deployment_name(), "text-embedding-ada-002"
        )

        defaults.reset_deployment_name()
        defaults.reset_subscription_key()
        defaults.reset_temperature()
        defaults.reset_seed()
        defaults.reset_top_p()
        defaults.reset_URL()
        defaults.reset_api_version()
        defaults.reset_model()
        defaults.reset_embedding_deployment_name()

        self.assertEqual(defaults.get_deployment_name(), None)
        self.assertEqual(defaults.get_subscription_key(), None)
        self.assertEqual(defaults.get_temperature(), None)
        self.assertEqual(defaults.get_seed(), None)
        self.assertEqual(defaults.get_top_p(), None)
        self.assertEqual(defaults.get_URL(), None)
        self.assertEqual(defaults.get_api_version(), None)
        self.assertEqual(defaults.get_model(), None)
        self.assertEqual(defaults.get_embedding_deployment_name(), None)

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

    def test_prompt_w_defaults(self):

        cmd = (
            "az keyvault secret show "
            "--vault-name mmlspark-build-keys "
            "--name openai-api-key-2"
        )
        secret_json = subprocess.check_output(cmd, shell=True)
        openai_api_key = json.loads(secret_json)["value"]

        df = spark.createDataFrame(
            [
                ("apple", "fruits"),
                ("mercedes", "cars"),
                ("cake", "dishes"),
            ],
            ["text", "category"],
        )

        defaults = OpenAIDefaults()
        defaults.set_deployment_name("gpt-4.1-mini")
        defaults.set_subscription_key(openai_api_key)
        defaults.set_temperature(0.05)
        defaults.set_URL("https://synapseml-openai-2.openai.azure.com/")

        prompt = OpenAIPrompt()
        prompt = prompt.setOutputCol("outParsed")
        prompt = prompt.setPromptTemplate(
            "Complete this comma separated list of 5 {category}: {text}, "
        )
        results = prompt.transform(df)
        results.select("outParsed").show(truncate=False)
        nonNullCount = results.filter(col("outParsed").isNotNull()).count()
        assert nonNullCount == 3

    def test_parameter_validation(self):
        defaults = OpenAIDefaults()

        # Test valid temperature values
        defaults.set_temperature(0.0)
        defaults.set_temperature(1.0)
        defaults.set_temperature(2.0)
        defaults.set_temperature(0)  # int should work
        defaults.set_temperature(1)  # int should work
        defaults.set_temperature(2)  # int should work

        # Test valid top_p values
        defaults.set_top_p(0.0)
        defaults.set_top_p(0.5)
        defaults.set_top_p(1.0)
        defaults.set_top_p(0)  # int should work
        defaults.set_top_p(1)  # int should work

        # Test invalid temperature values
        with self.assertRaises(ValueError):
            defaults.set_temperature(-0.1)
        with self.assertRaises(ValueError):
            defaults.set_temperature(2.1)

        # Test invalid top_p values
        with self.assertRaises(ValueError):
            defaults.set_top_p(-0.1)
        with self.assertRaises(ValueError):
            defaults.set_top_p(1.1)


class TestResponseFormatJsonSchema(unittest.TestCase):
    def setUp(self):
        self.schema_dict = {
            "type": "json_schema",
            "json_schema": {
                "name": "answer_schema",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "answer": {"type": "string"},
                    },
                    "required": ["answer"],
                    "additionalProperties": False,
                },
            },
        }
        import json as _json

        self.schema_json = _json.dumps(self.schema_dict)

    def test_prompt_set_response_format_dict(self):
        prompt = OpenAIPrompt().setResponseFormat(self.schema_dict)
        rf_type = prompt._java_obj.getResponseFormatType()
        self.assertEqual(rf_type, "json_schema")

    def test_prompt_set_response_format_json_string_rejected(self):
        prompt = OpenAIPrompt()
        with self.assertRaises(Exception):
            prompt.setResponseFormat(self.schema_json)

    def test_chat_set_response_format_dict(self):
        chat = OpenAIChatCompletion().setResponseFormat(self.schema_dict)
        rf_type = chat._java_obj.getResponseFormatType()
        self.assertEqual(rf_type, "json_schema")

    def test_chat_set_response_format_json_string_rejected(self):
        chat = OpenAIChatCompletion()
        with self.assertRaises(Exception):
            chat.setResponseFormat(self.schema_json)

    def test_chat_set_response_format_invalid_missing_schema(self):
        bad = {"type": "json_schema"}  # missing json_schema key
        chat = OpenAIChatCompletion()
        with self.assertRaises(Exception):
            chat.setResponseFormat(bad)

    def test_chat_set_response_format_invalid_type(self):
        bad = {"type": "not_a_valid_type"}
        chat = OpenAIChatCompletion()
        with self.assertRaises(Exception):
            chat.setResponseFormat(bad)

    def test_chat_set_response_format_bare_json_schema_string_rejected(self):
        chat = OpenAIChatCompletion()
        with self.assertRaises(Exception):
            chat.setResponseFormat("json_schema")

    def test_prompt_set_response_format_bare_json_schema_string_rejected(self):
        prompt = OpenAIPrompt()
        with self.assertRaises(Exception):
            prompt.setResponseFormat("json_schema")


if __name__ == "__main__":
    result = unittest.main()
