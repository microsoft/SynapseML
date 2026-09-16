# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import ast
import copy
import inspect
import json
import os
from pathlib import Path
import tempfile
import unittest

from pyspark.errors.exceptions.captured import IllegalArgumentException
from pyspark.sql.types import StructType

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai import (
    OpenAIChatCompletion,
    OpenAIPrompt,
    OpenAIResponses,
)
from synapse.ml.services.openai._OpenAIPrompt import _OpenAIPrompt

spark = init_spark()


class TestOpenAIResponseSchema(unittest.TestCase):
    def setUp(self):
        self.schema = {
            "type": "object",
            "properties": {
                "sentiment": {"type": "string", "enum": ["positive", "negative"]},
                "score": {
                    "type": "integer",
                    "minimum": -(2**63),
                    "maximum": 2**63 - 1,
                },
                "note": {"type": ["string", "null"], "enum": [None, "note"]},
            },
            "required": ["sentiment", "score", "note"],
            "additionalProperties": False,
        }
        self.stage_types = (OpenAIChatCompletion, OpenAIPrompt, OpenAIResponses)

    def response_format(self, stage):
        param = stage._java_obj.responseFormat()
        value = stage._java_obj.getOrDefault(param)
        payload = json.loads(param.jsonEncode(value))["left"]
        return payload.get("format", payload.get("json_schema", payload))

    def test_schema_only_setters_are_available_on_all_generated_stages(self):
        original = copy.deepcopy(self.schema)
        for stage_type in self.stage_types:
            with self.subTest(stage=stage_type.__name__):
                stage = stage_type()
                self.assertIs(stage.setResponseSchema(self.schema), stage)
                actual = self.response_format(stage)
                self.assertEqual(actual["name"], "response_schema")
                self.assertTrue(actual["strict"])
                self.assertEqual(actual["schema"], self.schema)
                self.assertFalse(stage.hasParam("responseSchema"))
        self.assertEqual(self.schema, original)

    def test_custom_name_and_non_strict_mode(self):
        for stage_type in self.stage_types:
            with self.subTest(stage=stage_type.__name__):
                stage = stage_type().setResponseSchema(
                    self.schema, name="sentiment-v2", strict=False
                )
                actual = self.response_format(stage)
                self.assertEqual(actual["name"], "sentiment-v2")
                self.assertFalse(actual["strict"])
                self.assertEqual(actual["schema"], self.schema)

    def test_generated_stubs_do_not_expose_nested_conversion_helpers(self):
        for stage_type in self.stage_types:
            with self.subTest(stage=stage_type.__name__):
                generated_type = (
                    _OpenAIPrompt if stage_type is OpenAIPrompt else stage_type
                )
                self.assertTrue(issubclass(stage_type, generated_type))
                stub = Path(inspect.getfile(generated_type)).with_suffix(".pyi")
                classes = [
                    node
                    for node in ast.parse(stub.read_text()).body
                    if isinstance(node, ast.ClassDef)
                    and node.name == generated_type.__name__
                ]
                self.assertEqual(len(classes), 1)
                methods = {
                    node.name
                    for node in classes[0].body
                    if isinstance(node, ast.FunctionDef)
                    and not node.name.startswith("_")
                }
                self.assertIn("setResponseSchema", methods)
                missing = sorted(
                    name for name in methods if not hasattr(stage_type, name)
                )
                self.assertEqual(missing, [])

    def test_schema_metadata_and_property_order_are_preserved(self):
        self.schema["name"] = "inner-schema-extension"
        self.schema["strict"] = "inner-schema-extension"
        self.schema["properties"]["category"] = {"type": "string"}
        detail_names = ["e_one", "d_two", "c_three", "b_four", "a_five", "z_six"]
        self.schema["properties"]["details"] = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {name: {"type": "string"} for name in detail_names},
                "required": detail_names,
                "additionalProperties": False,
            },
        }
        self.schema["required"].extend(["category", "details"])
        for stage_type in self.stage_types:
            with self.subTest(stage=stage_type.__name__):
                stage = stage_type().setResponseSchema(self.schema)
                actual = self.response_format(stage)["schema"]
                self.assertEqual(actual, self.schema)
                self.assertEqual(list(actual), list(self.schema))
                self.assertEqual(
                    list(actual["properties"]),
                    ["sentiment", "score", "note", "category", "details"],
                )
                self.assertEqual(
                    list(actual["properties"]["details"]["items"]["properties"]),
                    detail_names,
                )

    def test_invalid_schema_types_are_rejected_without_changing_parameters(self):
        for stage_type in self.stage_types:
            stage = stage_type().setResponseSchema(self.schema)
            expected = self.response_format(stage)
            for value in (
                None,
                [],
                "json_schema",
                json.dumps(self.schema),
                StructType(),
            ):
                with self.subTest(stage=stage_type.__name__, value=value):
                    with self.assertRaisesRegex(TypeError, "JSON Schema dictionary"):
                        stage.setResponseSchema(value)
                    self.assertEqual(self.response_format(stage), expected)
            with self.assertRaisesRegex(IllegalArgumentException, "non-empty"):
                stage.setResponseSchema({})
            self.assertEqual(self.response_format(stage), expected)

    def test_non_string_keys_are_rejected_without_coercion_or_parameter_changes(self):
        invalid_schemas = (
            {1: "invalid"},
            {None: "invalid"},
            {"properties": {1: {"type": "string"}}},
            {"properties": {None: {"type": "string"}}},
            {"properties": {1: {"type": "string"}, "1": {"type": "integer"}}},
            {"anyOf": [{1: {"type": "string"}}]},
        )
        for stage_type in self.stage_types:
            stage = stage_type().setResponseSchema(self.schema)
            expected = self.response_format(stage)
            for schema in invalid_schemas:
                with self.subTest(stage=stage_type.__name__, schema=schema):
                    with self.assertRaisesRegex(TypeError, "keys must be strings"):
                        stage.setResponseSchema(schema)
                    self.assertEqual(self.response_format(stage), expected)

    def test_invalid_names_and_strict_flags_are_rejected_atomically(self):
        for stage_type in self.stage_types:
            stage = stage_type().setResponseSchema(self.schema)
            expected = self.response_format(stage)
            for name in (None, "", "has spaces", "a" * 65):
                with self.subTest(stage=stage_type.__name__, name=name):
                    with self.assertRaisesRegex(
                        IllegalArgumentException, "schema name"
                    ):
                        stage.setResponseSchema(self.schema, name=name)
                    self.assertEqual(self.response_format(stage), expected)
            for strict in (None, 0, 1, "true"):
                with self.subTest(stage=stage_type.__name__, strict=strict):
                    with self.assertRaisesRegex(TypeError, "strict must be a boolean"):
                        stage.setResponseSchema(self.schema, strict=strict)
                    self.assertEqual(self.response_format(stage), expected)

    def test_copy_and_save_load_preserve_the_existing_response_format_parameter(self):
        for stage_type in self.stage_types:
            with self.subTest(stage=stage_type.__name__):
                stage = stage_type().setResponseSchema(
                    self.schema, name="persisted", strict=False
                )
                expected = self.response_format(stage)
                self.assertEqual(self.response_format(stage.copy({})), expected)
                with tempfile.TemporaryDirectory() as directory:
                    path = os.path.join(directory, "stage")
                    stage.write().save(path)
                    key = "spark.synapseml.legacy.allowUnsafeJavaDeserialization"
                    previous = spark.conf.get(key, None)
                    try:
                        # Only load the trusted model created by this test.
                        spark.conf.set(key, "true")
                        restored = stage_type.load(path)
                    finally:
                        if previous is None:
                            spark.conf.unset(key)
                        else:
                            spark.conf.set(key, previous)
                    self.assertEqual(self.response_format(restored), expected)

    def test_legacy_bare_schema_strictness_is_unchanged(self):
        for stage_type in self.stage_types:
            with self.subTest(stage=stage_type.__name__):
                stage = stage_type().setResponseFormat(self.schema)
                self.assertNotIn("strict", self.response_format(stage))
                stage.setResponseSchema(self.schema)
                self.assertTrue(self.response_format(stage)["strict"])

    def test_text_and_json_object_selectors_do_not_get_schema_envelopes(self):
        for stage_type in self.stage_types:
            for token in ("text", "json_object"):
                for value in (token, {"type": token}):
                    with self.subTest(stage=stage_type.__name__, value=value):
                        stage = stage_type().setResponseFormat(value)
                        self.assertEqual(self.response_format(stage), {"type": token})

    def test_named_partial_and_full_formats_preserve_name_and_strictness(self):
        for strict in (None, False, True):
            metadata = {"name": "ai_function_schema", "schema": self.schema}
            if strict is not None:
                metadata["strict"] = strict
            formats = (
                metadata,
                {"type": "json_schema", **metadata},
                {"type": "json_schema", "json_schema": metadata},
            )
            for stage_type in self.stage_types:
                for value in formats:
                    with self.subTest(
                        stage=stage_type.__name__, strict=strict, value=value
                    ):
                        stage = stage_type().setResponseFormat(value)
                        actual = self.response_format(stage)
                        self.assertEqual(actual["name"], "ai_function_schema")
                        self.assertEqual(actual["schema"], self.schema)
                        if strict is None:
                            self.assertNotIn("strict", actual)
                        else:
                            self.assertIs(actual["strict"], strict)


if __name__ == "__main__":
    unittest.main()
