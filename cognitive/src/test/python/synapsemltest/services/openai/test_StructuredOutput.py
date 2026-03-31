# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import subprocess
import unittest

from pyspark.sql import SQLContext

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai import OpenAIDefaults, OpenAIPrompt

spark = init_spark()
sc = SQLContext(spark.sparkContext)


def _make_json_schema(reason_first: bool) -> dict:
    props = {
        "ans": {"type": "string"},
        "reason": {"type": "string"},
    }
    if reason_first:
        props = {
            "reason": {"type": "string"},
            "ans": {"type": "string"},
        }

    return {
        "type": "object",
        "properties": props,
        "required": list(props.keys()),
        "additionalProperties": False,
    }


def _assert_valid_response(test_case, text):
    """Validate that the model response is valid JSON with the expected keys.

    We intentionally do NOT assert on the ordering of keys in the model
    response. OpenAI does not guarantee that response key order matches schema
    property order. Schema ordering preservation (Python dict -> LinkedHashMap
    -> serialized JSON request) is tested by the Scala ResponseFormatOrderSuite.
    """
    test_case.assertIsInstance(text, str, "Response should be a string")
    test_case.assertTrue(len(text) > 0, "Response should not be empty")
    json_response = json.loads(text)
    test_case.assertIsInstance(
        json_response, dict, f"Response should be a JSON object: {text}"
    )
    test_case.assertIn("reason", json_response, f"Missing 'reason' key: {text}")
    test_case.assertIn("ans", json_response, f"Missing 'ans' key: {text}")
    test_case.assertIsInstance(
        json_response["reason"], str, f"'reason' should be a string: {text}"
    )
    test_case.assertIsInstance(
        json_response["ans"], str, f"'ans' should be a string: {text}"
    )


class TestStructuredOutput(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        defaults = OpenAIDefaults()
        defaults.reset_model()

        cls.subscriptionKey = json.loads(
            subprocess.check_output(
                "az keyvault secret show --vault-name mmlspark-build-keys"
                " --name openai-api-key-2",
                shell=True,
            )
        )["value"]
        cls.url = "https://synapseml-openai-2.openai.azure.com/"
        cls.api_version = "2025-04-01-preview"
        cls.deploymentName = "gpt-4.1-mini"

        cls.df = spark.createDataFrame([("Paris", "City")], ["text", "category"])

    def _make_prompt(self, api_type, reason_first):
        return (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType(api_type)
            .setResponseFormat(_make_json_schema(reason_first=reason_first))
            .setOutputCol("out")
            .setDeploymentName(self.deploymentName)
            .setApiVersion(self.api_version)
            .setUrl(self.url)
            .setSubscriptionKey(self.subscriptionKey)
        )

    def test_chat_structured_output_reason_first(self):
        prompt = self._make_prompt("chat_completions", reason_first=True)
        text = prompt.transform(self.df).select("out").first()[0]
        _assert_valid_response(self, text)

    def test_chat_structured_output_ans_first(self):
        prompt = self._make_prompt("chat_completions", reason_first=False)
        text = prompt.transform(self.df).select("out").first()[0]
        _assert_valid_response(self, text)

    def test_responses_structured_output_reason_first(self):
        prompt = self._make_prompt("responses", reason_first=True)
        text = prompt.transform(self.df).select("out").first()[0]
        _assert_valid_response(self, text)

    def test_responses_structured_output_ans_first(self):
        prompt = self._make_prompt("responses", reason_first=False)
        text = prompt.transform(self.df).select("out").first()[0]
        _assert_valid_response(self, text)


if __name__ == "__main__":
    unittest.main()
