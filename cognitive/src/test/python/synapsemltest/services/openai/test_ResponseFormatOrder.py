# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import os
import subprocess
import unittest

from pyspark.sql import SQLContext

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai import OpenAIPrompt

spark = init_spark()
sc = SQLContext(spark.sparkContext)


def _make_json_schema(reason_first: bool) -> dict:
    # Build an ordered dict-like structure in Python
    # Python dict preserves insertion order; we rely on wrapper converting to LinkedHashMap recursively.
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


class TestResponseFormatOrder(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        # fetching openai_api_key from azure keyvault
        openai_api_key = json.loads(
            subprocess.check_output(
                "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key-2",
                shell=True,
            )
        )["value"]
        openai_api_base = "https://synapseml-openai-2.openai.azure.com/"
        openai_api_version = "2025-04-01-preview"

        os.environ["OPENAI_API_VERSION"] = openai_api_version
        os.environ["OPENAI_API_BASE"] = openai_api_base
        os.environ["OPENAI_API_KEY"] = openai_api_key

        self.subscriptionKey = openai_api_key
        self.url = openai_api_base
        self.api_version = openai_api_version
        self.deploymentName = "gpt-4.1-mini"

        # construction of test dataframe
        self.df = spark.createDataFrame([("Paris", "City")], ["text", "category"])

    def test_chat_reason_then_ans(self):

        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("chat_completions")
            .setResponseFormat(_make_json_schema(reason_first=True))
            .setOutputCol("out")
            .setdeploymentName(self.deploymentName)
            .setApiVersion(self.api_version)
            .setUrl(self.url)
            .setSubscriptionKey(self.subscriptionKey)
        )
        out_df = prompt.transform(self.df)
        text = out_df.select("out").first()[0]

        self.assertIsInstance(text, str)
        json_response = json.loads(text)
        self.assertIsNotNone(json_response)
        self.assertIsInstance(json_response, dict)

        reason_index = text.find('"reason"')
        answer_index = text.find('"ans"')
        self.assertTrue(
            reason_index >= 0 and answer_index >= 0,
            f"reason/ans keys not found: {text}",
        )
        self.assertTrue(
            reason_index < answer_index, f"Expected reason before ans. Output: {text}"
        )

    def test_chat_ans_then_reason(self):

        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("chat_completions")
            .setResponseFormat(_make_json_schema(reason_first=False))
            .setOutputCol("out")
            .setdeploymentName(self.deploymentName)
            .setApiVersion(self.api_version)
            .setUrl(self.url)
            .setSubscriptionKey(self.subscriptionKey)
        )
        out_df = prompt.transform(self.df)
        text = out_df.select("out").first()[0]

        self.assertIsInstance(text, str)
        json_response = json.loads(text)
        self.assertIsNotNone(json_response)
        self.assertIsInstance(json_response, dict)

        reason_index = text.find('"reason"')
        answer_index = text.find('"ans"')
        self.assertTrue(
            reason_index >= 0 and answer_index >= 0,
            f"reason/ans keys not found: {text}",
        )
        self.assertTrue(
            reason_index > answer_index, f"Expected ans before reason. Output: {text}"
        )

    def test_responses_reason_then_ans(self):

        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("responses")
            .setResponseFormat(_make_json_schema(reason_first=True))
            .setOutputCol("out")
            .setdeploymentName(self.deploymentName)
            .setApiVersion(self.api_version)
            .setUrl(self.url)
            .setSubscriptionKey(self.subscriptionKey)
        )
        out_df = prompt.transform(self.df)
        text = out_df.select("out").first()[0]

        self.assertIsInstance(text, str)
        json_response = json.loads(text)
        self.assertIsNotNone(json_response)
        self.assertIsInstance(json_response, dict)

        reason_index = text.find('"reason"')
        answer_index = text.find('"ans"')
        self.assertTrue(
            reason_index >= 0 and answer_index >= 0,
            f"reason/ans keys not found: {text}",
        )
        self.assertTrue(
            reason_index < answer_index, f"Expected reason before ans. Output: {text}"
        )

    def test_responses_ans_then_reason(self):

        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("responses")
            .setResponseFormat(_make_json_schema(reason_first=False))
            .setOutputCol("out")
            .setdeploymentName(self.deploymentName)
            .setApiVersion(self.api_version)
            .setUrl(self.url)
            .setSubscriptionKey(self.subscriptionKey)
        )
        out_df = prompt.transform(self.df)
        text = out_df.select("out").first()[0]

        self.assertIsInstance(text, str)
        json_response = json.loads(text)
        self.assertIsNotNone(json_response)
        self.assertIsInstance(json_response, dict)

        reason_index = text.find('"reason"')
        answer_index = text.find('"ans"')
        self.assertTrue(
            reason_index >= 0 and answer_index >= 0,
            f"reason/ans keys not found: {text}",
        )
        self.assertTrue(
            reason_index > answer_index, f"Expected ans before reason. Output: {text}"
        )


if __name__ == "__main__":
    unittest.main()
