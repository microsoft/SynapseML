# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai import OpenAIChatCompletion, OpenAIResponses


spark = init_spark()


def _entity_to_string(entity):
    is_ = entity.getContent()
    try:
        # Read Java InputStream to Python string
        # Using JDK classes via Py4J
        java_scanner = spark.sparkContext._jvm.java.util.Scanner(is_, "UTF-8").useDelimiter("\\A")
        return java_scanner.next()
    finally:
        is_.close()


def _make_json_schema(reason_first: bool):
    # Build an ordered dict-like structure in Python
    # Python dict preserves insertion order; we rely on wrapper converting to LinkedHashMap recursively.
    props = ({
        "reason": {"type": "string"},
        "ans": {"type": "string"},
    } if reason_first else {
        "ans": {"type": "string"},
        "reason": {"type": "string"},
    })

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
        chat = OpenAIChatCompletion().setResponseFormat(_make_json_schema(True))

        # Build Java LinkedHashMap optional params with response_format
        jvm = spark.sparkContext._jvm
        opt = jvm.java.util.LinkedHashMap()
        opt.put("response_format", chat._java_obj.getResponseFormat())

        empty_scala_seq = jvm.scala.collection.JavaConverters.asScalaBuffer([]).toSeq()
        entity = chat._java_obj.getStringEntity(empty_scala_seq, opt)
        json = _entity_to_string(entity)

        props_start = json.find("\"properties\":{")
        self.assertTrue(props_start >= 0, f"properties block not found in JSON: {json}")
        ridx = json.find("\"reason\"", props_start)
        aidx = json.find("\"ans\"", props_start)
        self.assertTrue(ridx >= 0 and aidx >= 0, f"reason/ans keys not found in properties: {json}")
        self.assertTrue(ridx < aidx, f"Order incorrect: expected reason before ans. JSON: {json}")

    def test_chat_ans_then_reason(self):
        chat = OpenAIChatCompletion().setResponseFormat(_make_json_schema(False))

        jvm = spark.sparkContext._jvm
        opt = jvm.java.util.LinkedHashMap()
        opt.put("response_format", chat._java_obj.getResponseFormat())

        empty_scala_seq = jvm.scala.collection.JavaConverters.asScalaBuffer([]).toSeq()
        entity = chat._java_obj.getStringEntity(empty_scala_seq, opt)
        json = _entity_to_string(entity)

        props_start = json.find("\"properties\":{")
        self.assertTrue(props_start >= 0, f"properties block not found in JSON: {json}")
        ridx = json.find("\"reason\"", props_start)
        aidx = json.find("\"ans\"", props_start)
        self.assertTrue(ridx >= 0 and aidx >= 0, f"reason/ans keys not found in properties: {json}")
        self.assertTrue(aidx < ridx, f"Order incorrect: expected ans before reason. JSON: {json}")

    def test_responses_reason_then_ans(self):
        resp = OpenAIResponses().setResponseFormat(_make_json_schema(True))

        jvm = spark.sparkContext._jvm
        empty_scala_seq = jvm.scala.collection.JavaConverters.asScalaBuffer([]).toSeq()

        # For Responses API, setResponseFormat already wraps the dict as top-level 'format'
        entity = resp._java_obj.getStringEntity(empty_scala_seq, resp._java_obj.getResponseFormat())
        json = _entity_to_string(entity)

        props_start = json.find("\"properties\":{")
        self.assertTrue(props_start >= 0, f"properties block not found in JSON: {json}")
        ridx = json.find("\"reason\"", props_start)
        aidx = json.find("\"ans\"", props_start)
        self.assertTrue(ridx >= 0 and aidx >= 0, f"reason/ans keys not found in properties: {json}")
        self.assertTrue(ridx < aidx, f"Order incorrect: expected reason before ans. JSON: {json}")

    def test_responses_ans_then_reason(self):
        resp = OpenAIResponses().setResponseFormat(_make_json_schema(False))

        jvm = spark.sparkContext._jvm
        empty_scala_seq = jvm.scala.collection.JavaConverters.asScalaBuffer([]).toSeq()

        entity = resp._java_obj.getStringEntity(empty_scala_seq, resp._java_obj.getResponseFormat())
        json = _entity_to_string(entity)

        props_start = json.find("\"properties\":{")
        self.assertTrue(props_start >= 0, f"properties block not found in JSON: {json}")
        ridx = json.find("\"reason\"", props_start)
        aidx = json.find("\"ans\"", props_start)
        self.assertTrue(ridx >= 0 and aidx >= 0, f"reason/ans keys not found in properties: {json}")
        self.assertTrue(aidx < ridx, f"Order incorrect: expected ans before reason. JSON: {json}")


if __name__ == "__main__":
    unittest.main()

