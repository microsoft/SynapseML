# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import os
import shutil
import unittest

from pyspark.sql import functions as F
from pyspark.sql.column import Column

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai import OpenAIPrompt, OpenAIResponses

spark = init_spark()


WEATHER_TOOL = {
    "type": "function",
    "name": "get_weather",
    "description": "Get the current weather for a city.",
    "parameters": {
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
        "additionalProperties": False,
    },
    "strict": True,
}


class TestOpenAIResponsesTools(unittest.TestCase):
    def test_python_tool_setters_round_trip_json(self):
        transformer = (
            OpenAIResponses()
            .setTools([WEATHER_TOOL])
            .addFunctionTool(
                "get_forecast",
                "Get a forecast.",
                {"type": "object", "properties": {}},
            )
            .addFunctionTool(
                "get_alerts",
                "Get weather alerts.",
                {"type": "object", "properties": {}},
            )
            .setToolChoice({"type": "function", "name": "get_weather"})
            .setParallelToolCalls(False)
            .setMaxToolCalls(2)
            .setInclude(["reasoning.encrypted_content"])
        )
        tools = transformer.getToolsAsList()
        self.assertEqual(
            [tool["name"] for tool in tools],
            ["get_weather", "get_forecast", "get_alerts"],
        )
        self.assertEqual(tools[0]["name"], "get_weather")
        self.assertEqual(tools[0]["parameters"]["required"], ["city"])
        self.assertEqual(json.loads(transformer.getToolChoice())["name"], "get_weather")
        self.assertFalse(transformer.getParallelToolCalls())
        self.assertEqual(transformer.getMaxToolCalls(), 2)
        self.assertEqual(transformer.getInclude(), ["reasoning.encrypted_content"])

        nullable = OpenAIResponses().setTools(
            {
                "type": "function",
                "name": "nullable",
                "parameters": None,
                "strict": None,
            }
        )
        self.assertIsNone(nullable.getToolsAsList()[0]["parameters"])
        self.assertIsNone(nullable.getToolsAsList()[0]["strict"])

    def test_python_type_errors_and_column_setters(self):
        transformer = OpenAIResponses()
        self.assertEqual(transformer.getToolsAsList(), [])
        for value in (5, None):
            with self.assertRaises(TypeError):
                transformer.setTools(value)
        for value in (["auto"], 3):
            with self.assertRaises(TypeError):
                transformer.setToolChoice(value)

        transformer.setToolsCol("tools_json")
        transformer.setToolChoiceCol("choice_json")
        transformer.setParallelToolCallsCol("parallel")
        transformer.setMaxToolCallsCol("max_calls")
        transformer.setInputItemsCol("items")
        transformer.setFunctionCallOutputsCol("outputs")
        transformer.setToolCallsCol("calls")
        self.assertEqual(transformer.getToolsCol(), "tools_json")
        self.assertEqual(transformer.getToolChoiceCol(), "choice_json")
        self.assertEqual(transformer.getParallelToolCallsCol(), "parallel")
        self.assertEqual(transformer.getMaxToolCallsCol(), "max_calls")
        self.assertEqual(transformer.getInputItemsCol(), "items")
        self.assertEqual(transformer.getFunctionCallOutputsCol(), "outputs")
        self.assertEqual(transformer.getToolCallsCol(), "calls")
        with self.assertRaisesRegex(ValueError, "column-bound.*tools_json"):
            transformer.getToolsAsList()

    def test_tool_calls_column_and_save_load(self):
        response_json = json.dumps(
            {
                "id": "resp_1",
                "object": "response",
                "created_at": "1",
                "model": "gpt-5.1",
                "output": [
                    {
                        "type": "function_call",
                        "id": "fc_1",
                        "call_id": "call_a",
                        "name": "get_weather",
                        "arguments": '{"city":"Seattle"}',
                        "status": "completed",
                    }
                ],
            }
        )
        response = spark.read.json(spark.sparkContext.parallelize([response_json]))
        wrapped = response.select(F.struct(*response.columns).alias("out"))
        transformer = (
            OpenAIResponses()
            .setTools([WEATHER_TOOL])
            .addFunctionTool(
                "get_forecast",
                "Get a forecast.",
                {"type": "object", "properties": {}},
            )
            .addFunctionTool(
                "get_alerts",
                "Get weather alerts.",
                {"type": "object", "properties": {}},
            )
            .setToolChoice("auto")
        )
        calls = wrapped.select(
            transformer.toolCallsColumn("out").alias("tool_calls")
        ).first()["tool_calls"]
        self.assertEqual(calls[0]["call_id"], "call_a")
        self.assertEqual(calls[0]["name"], "get_weather")

        path = os.path.join(
            os.getcwd(), "target", "python-test-openai-responses-tools-model"
        )
        shutil.rmtree(path, ignore_errors=True)
        try:
            transformer.setMaxToolCalls(2).setMetadata({"suite": "python"})
            transformer.write().overwrite().save(path)
            loaded = OpenAIResponses.load(path)
            self.assertEqual(
                [tool["name"] for tool in loaded.getToolsAsList()],
                ["get_weather", "get_forecast", "get_alerts"],
            )
            self.assertEqual(loaded.getToolChoice(), "auto")
            self.assertEqual(loaded.getMaxToolCalls(), 2)
            self.assertEqual(dict(loaded.getMetadata()), {"suite": "python"})
        finally:
            shutil.rmtree(path, ignore_errors=True)

    def test_prompt_surface_and_responses_only_validation(self):
        prompt = (
            OpenAIPrompt()
            .setApiType("responses")
            .setTools([WEATHER_TOOL])
            .setToolCallsCol("tool_calls")
            .setResponseStructCol("response_struct")
            .setReasoningSummary("concise")
        )
        self.assertEqual(prompt.getToolsAsList()[0]["name"], "get_weather")
        self.assertEqual(prompt.getToolCallsCol(), "tool_calls")
        self.assertEqual(prompt.getResponseStructCol(), "response_struct")
        self.assertEqual(prompt.getReasoningSummary(), "concise")

        invalid = OpenAIPrompt().setPromptTemplate("{city}").setTools([WEATHER_TOOL])
        with self.assertRaisesRegex(Exception, "requires apiType='responses'"):
            invalid.transform(spark.createDataFrame([("Seattle",)], ["city"]))

    def test_prompt_column_helpers_require_or_use_response_struct(self):
        prompt = OpenAIPrompt()
        with self.assertRaisesRegex(
            ValueError,
            "toolCallsColumn requires outputCol or a configured responseStructCol",
        ):
            prompt.toolCallsColumn()
        with self.assertRaisesRegex(
            ValueError,
            "replayItemsColumn requires outputCol or a configured responseStructCol",
        ):
            prompt.replayItemsColumn()

        configured = prompt.setResponseStructCol("out")
        self.assertIsInstance(configured.toolCallsColumn(), Column)
        self.assertIsInstance(configured.replayItemsColumn(), Column)

        response_json = json.dumps(
            {
                "output": [
                    {
                        "type": "function_call",
                        "id": "fc_1",
                        "call_id": "call_a",
                        "name": "get_weather",
                        "arguments": '{"city":"Seattle"}',
                        "status": "completed",
                    }
                ]
            }
        )
        response = spark.read.json(spark.sparkContext.parallelize([response_json]))
        wrapped = response.select(F.struct(*response.columns).alias("out"))
        calls = wrapped.select(
            configured.toolCallsColumn().alias("tool_calls")
        ).first()["tool_calls"]
        self.assertEqual(calls[0]["call_id"], "call_a")

        explicit = OpenAIPrompt()
        self.assertIsInstance(explicit.toolCallsColumn("out"), Column)


if __name__ == "__main__":
    unittest.main()
