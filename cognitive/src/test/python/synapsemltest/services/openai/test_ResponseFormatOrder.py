# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import unittest

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai import OpenAIPrompt, OpenAIResponses

spark = init_spark()


def _entity_to_string(entity):
    is_ = entity.getContent()
    try:
        # Read Java InputStream to Python string
        # Using JDK classes via Py4J
        java_scanner = spark.sparkContext._jvm.java.util.Scanner(
            is_, "UTF-8"
        ).useDelimiter("\\A")
        return java_scanner.next()
    finally:
        is_.close()


def _make_json_schema(reason_first: bool) -> dict:
    # Build an ordered dict-like structure in Python
    # Python dict preserves insertion order; we rely on wrapper converting to LinkedHashMap recursively.
    props = (
        {
            "reason": {"type": "string"},
            "ans": {"type": "string"},
        }
        if reason_first
        else {
            "ans": {"type": "string"},
            "reason": {"type": "string"},
        }
    )

    return {
        "type": "json_schema",
        "json_schema": {
            "name": "ordered_schema",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": props,
                "required": list(props.keys()),
                "additionalProperties": False,
            },
        },
    }


class TestResponseFormatOrder(unittest.TestCase):
    def test_chat_reason_then_ans(self):

        df = spark.createDataFrame([("Paris", "City")], ["text", "category"])
        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("chat_completions")
            .setResponseFormat(_make_json_schema(reason_first=True))
            .setOutputCol("out")
        )
        out_df = prompt.transform(df)
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

        df = spark.createDataFrame([("item", "category")], ["text", "category"])
        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("chat_completions")
            .setResponseFormat(_make_json_schema(reason_first=False))
            .setOutputCol("out")
        )
        out_df = prompt.transform(df)
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

        df = spark.createDataFrame([("Paris", "City")], ["text", "category"])
        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("responses")
            .setResponseFormat(_make_json_schema(reason_first=True))
            .setOutputCol("out")
        )
        out_df = prompt.transform(df)
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

        df = spark.createDataFrame([("item", "category")], ["text", "category"])
        prompt = (
            OpenAIPrompt()
            .setPromptTemplate("List 2 {category}: {text},")
            .setApiType("responses")
            .setResponseFormat(_make_json_schema(reason_first=False))
            .setOutputCol("out")
        )
        out_df = prompt.transform(df)
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
