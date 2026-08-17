# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import glob
import json
import os
import tempfile
import unittest

from pyspark.errors.exceptions.captured import IllegalArgumentException
from pyspark.sql.types import StructType

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai.OpenAIPrompt import OpenAIPrompt

spark = init_spark()


class TestOpenAIPromptParams(unittest.TestCase):
    def test_empty_post_processing_options_remain_compatible(self):
        prompts = [
            OpenAIPrompt(postProcessingOptions={}),
            OpenAIPrompt().setPostProcessingOptions({}),
        ]

        for prompt in prompts:
            self.assertEqual(prompt.getPostProcessing(), "")
            self.assertEqual(prompt.getPostProcessingOptions(), {})

    def test_constructor_preserves_input_kwargs(self):
        options = {"delimiter": ";"}
        prompt = OpenAIPrompt(postProcessingOptions=options)

        self.assertEqual(prompt._input_kwargs["postProcessingOptions"], options)

    def test_set_post_processing_options_rejects_malformed_values(self):
        malformed_values = [
            None,
            [],
            "",
            {"delimiter": 1},
            {1: ","},
        ]

        for value in malformed_values:
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    OpenAIPrompt().setPostProcessingOptions(value)

        with self.assertRaises(TypeError):
            OpenAIPrompt().setParams(postProcessingOptions=None)

        for action in [
            lambda: OpenAIPrompt().setPostProcessing(1),
            lambda: OpenAIPrompt().setParams(postProcessing=1),
            lambda: OpenAIPrompt(postProcessing=1),
        ]:
            with self.subTest(action=action):
                with self.assertRaisesRegex(
                    TypeError,
                    'Invalid param value given for param "postProcessing"',
                ):
                    action()

    def test_unsupported_post_processing_is_rejected_atomically(self):
        prompt = OpenAIPrompt().setPostProcessing("csv")

        for action in [
            lambda: prompt.setPostProcessing("bogus"),
            lambda: prompt.setParams(postProcessing="bogus"),
            lambda: prompt.setParams(
                postProcessing="bogus",
                postProcessingOptions={"delimiter": ";"},
            ),
            lambda: prompt.setParams(
                postProcessing="bogus",
                postProcessingOptions={"invalidOption": "x"},
            ),
        ]:
            with self.subTest(action=action):
                with self.assertRaisesRegex(
                    IllegalArgumentException,
                    "Unsupported postProcessing mode 'bogus'",
                ):
                    action()
                self.assertEqual(prompt.getPostProcessing(), "csv")
                self.assertFalse(prompt.isSet(prompt.postProcessingOptions))

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Unsupported postProcessing mode 'bogus'",
        ):
            OpenAIPrompt(postProcessing="bogus")

    def test_set_post_processing_options_infers_csv_and_json_modes(self):
        cases = [
            ({"delimiter": ";"}, "csv"),
            ({"jsonSchema": "value STRING"}, "json"),
        ]

        for options, expected_mode in cases:
            with self.subTest(expected_mode=expected_mode):
                prompt = OpenAIPrompt().setPostProcessingOptions(options)

                self.assertEqual(prompt.getPostProcessing(), expected_mode)
                self.assertEqual(prompt.getPostProcessingOptions(), options)
                self.assertEqual(prompt._java_obj.getPostProcessing(), "")
                prompt._transfer_params_to_java()
                self.assertEqual(prompt._java_obj.getPostProcessing(), expected_mode)

    def test_set_post_processing_options_accepts_valid_regex(self):
        options = {"regex": "value=(.*)", "regexGroup": "1"}
        prompt = OpenAIPrompt().setPostProcessingOptions(options)

        self.assertEqual(prompt.getPostProcessing(), "regex")
        self.assertEqual(prompt.getPostProcessingOptions(), options)
        self.assertEqual(prompt._java_obj.getPostProcessing(), "")
        prompt._transfer_params_to_java()
        self.assertEqual(prompt._java_obj.getPostProcessing(), "regex")

    def test_set_post_processing_options_rejects_regex_without_group(self):
        prompt = OpenAIPrompt()

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "regexGroup must be specified with regex",
        ):
            prompt.setPostProcessingOptions({"regex": ".*"})

    def test_set_post_processing_options_rejects_invalid_values(self):
        cases = [
            ({"jsonSchema": "not a schema"}, "Invalid jsonSchema"),
            ({"jsonSchema": "STRING"}, "Invalid jsonSchema"),
            ({"jsonSchema": "MAP<INT, STRING>"}, "Invalid jsonSchema"),
            (
                {"jsonSchema": "STRUCT<x: MAP<INT, STRING>>"},
                "Invalid jsonSchema",
            ),
            (
                {"jsonSchema": "STRUCT<x: VARCHAR(10)>"},
                "Invalid jsonSchema",
            ),
            ({"delimiter": "["}, "Invalid delimiter"),
            ({"regex": "([", "regexGroup": "1"}, "Invalid regex"),
            (
                {"regex": "(.*)", "regexGroup": "not-an-integer"},
                "regexGroup must be a non-negative integer",
            ),
            (
                {"regex": "(.*)", "regexGroup": "2"},
                "regexGroup exceeds the number of capture groups",
            ),
            (
                {"delimiter": ",", "jsonSchema": "value STRING"},
                "Invalid post processing options",
            ),
            (
                {"delimiter": ",", "regexGroup": "0"},
                "Invalid post processing options",
            ),
        ]

        for options, message in cases:
            with self.subTest(options=options):
                with self.assertRaisesRegex(IllegalArgumentException, message):
                    OpenAIPrompt().setPostProcessingOptions(options)

    def test_json_and_regex_modes_require_options(self):
        cases = [
            (
                "json",
                "jsonSchema must be specified with json postProcessing",
            ),
            (
                "regex",
                "regex and regexGroup must be specified with regex postProcessing",
            ),
        ]

        for mode, message in cases:
            with self.subTest(mode=mode):
                with self.assertRaisesRegex(IllegalArgumentException, message):
                    OpenAIPrompt(
                        postProcessing=mode,
                        postProcessingOptions={},
                    )

                prompt = OpenAIPrompt().setPostProcessing(mode)
                with self.assertRaisesRegex(IllegalArgumentException, message):
                    prompt.setPostProcessingOptions({})

                with self.assertRaisesRegex(IllegalArgumentException, message):
                    OpenAIPrompt().setParams(
                        postProcessing=mode,
                        postProcessingOptions={},
                    )

    def test_set_params_validates_post_processing_options_on_jvm(self):
        prompt = OpenAIPrompt().setParams(
            postProcessingOptions={"delimiter": ";"},
        )

        self.assertEqual(prompt.getPostProcessing(), "csv")
        self.assertEqual(prompt._java_obj.getPostProcessing(), "")
        prompt._transfer_params_to_java()
        self.assertEqual(prompt._java_obj.getPostProcessing(), "csv")

        conflicting_prompt = OpenAIPrompt()
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "postProcessing must be 'csv'",
        ):
            conflicting_prompt.setParams(
                postProcessing="json",
                postProcessingOptions={"delimiter": ";"},
            )

        self.assertFalse(conflicting_prompt.isSet(conflicting_prompt.postProcessing))
        self.assertEqual(conflicting_prompt._java_obj.getPostProcessing(), "")

    def test_clear_does_not_leave_eager_java_state(self):
        prompt = OpenAIPrompt().setPostProcessingOptions({"delimiter": ";"})

        self.assertEqual(prompt.getPostProcessing(), "csv")
        self.assertEqual(prompt._java_obj.getPostProcessing(), "")

        prompt.clear(prompt.postProcessing)
        prompt._transfer_params_to_java()

        self.assertFalse(prompt.isSet(prompt.postProcessing))
        self.assertTrue(prompt.isSet(prompt.postProcessingOptions))
        self.assertEqual(prompt.getPostProcessing(), "")
        self.assertEqual(prompt.getPostProcessingOptions(), {"delimiter": ";"})
        self.assertEqual(prompt._java_obj.getPostProcessing(), "")
        self.assertEqual(
            prompt._java_obj.getPostProcessingOptions().apply("delimiter"),
            ";",
        )

        empty_schema = spark._jsparkSession.parseDataType(StructType([]).json())
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "postProcessing must be 'csv'",
        ):
            prompt._java_obj.transformSchema(empty_schema)

    def test_legacy_options_only_stage_loads_and_infers_mode(self):
        legacy_prompt = OpenAIPrompt().setPostProcessingOptions(
            {"delimiter": ";"},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "legacy-openai-prompt")
            legacy_prompt.save(path)
            metadata_path = glob.glob(os.path.join(path, "metadata", "part-*"))[0]
            with open(metadata_path, encoding="utf-8") as metadata_file:
                metadata = json.load(metadata_file)
            metadata["paramMap"]["postProcessing"] = ""
            with open(metadata_path, "w", encoding="utf-8") as metadata_file:
                json.dump(metadata, metadata_file)
            checksum_path = os.path.join(
                os.path.dirname(metadata_path),
                f".{os.path.basename(metadata_path)}.crc",
            )
            if os.path.exists(checksum_path):
                os.remove(checksum_path)

            loaded_prompt = OpenAIPrompt.load(path)

            self.assertEqual(loaded_prompt.getPostProcessing(), "")
            self.assertEqual(
                loaded_prompt._java_obj.getPostProcessingOptions().apply("delimiter"),
                ";",
            )

            empty_schema = spark._jsparkSession.parseDataType(StructType([]).json())
            transformed_schema = loaded_prompt._java_obj.transformSchema(empty_schema)
            output_type = transformed_schema.apply(
                loaded_prompt.getOutputCol()
            ).dataType()
            self.assertEqual(output_type.typeName(), "array")

            loaded_options = loaded_prompt.getPostProcessingOptions()
            loaded_prompt.setPostProcessingOptions(loaded_options)
            self.assertEqual(loaded_prompt.getPostProcessing(), "csv")
            self.assertEqual(
                loaded_prompt.getPostProcessingOptions(),
                {"delimiter": ";"},
            )

            with self.assertRaisesRegex(
                IllegalArgumentException,
                "postProcessing must be 'csv'",
            ):
                loaded_prompt.setPostProcessing("json")

            loaded_prompt.setPostProcessing("csv")

            explicit_empty_prompt = OpenAIPrompt().setPostProcessing("")
            with self.assertRaisesRegex(
                IllegalArgumentException,
                "postProcessing must be 'csv'",
            ):
                explicit_empty_prompt.setPostProcessingOptions(loaded_options)

            with self.assertRaisesRegex(
                IllegalArgumentException,
                "postProcessing must be 'csv'",
            ):
                OpenAIPrompt(
                    postProcessing="",
                    postProcessingOptions=loaded_options,
                )

            with self.assertRaisesRegex(
                IllegalArgumentException,
                "postProcessing must be 'csv'",
            ):
                OpenAIPrompt().setParams(
                    postProcessing="",
                    postProcessingOptions=loaded_options,
                )

    def test_reverse_order_mode_changes_fail_immediately(self):
        prompt = OpenAIPrompt().setPostProcessingOptions({"delimiter": ";"})

        for mode in ["", "json"]:
            with self.subTest(mode=mode):
                with self.assertRaisesRegex(
                    IllegalArgumentException,
                    "postProcessing must be 'csv'",
                ):
                    prompt.setPostProcessing(mode)

                with self.assertRaisesRegex(
                    IllegalArgumentException,
                    "postProcessing must be 'csv'",
                ):
                    prompt.setParams(postProcessing=mode)

                self.assertEqual(prompt.getPostProcessing(), "csv")
                self.assertEqual(
                    prompt.getPostProcessingOptions(),
                    {"delimiter": ";"},
                )

    def test_copy_preserves_explicit_mode_provenance(self):
        source = OpenAIPrompt()
        copied = source.copy({source.postProcessing: ""})

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "postProcessing must be 'csv'",
        ):
            copied.setPostProcessingOptions({"delimiter": ";"})

        self.assertEqual(copied.getPostProcessing(), "")
        self.assertEqual(copied.getPostProcessingOptions(), {})

        csv_source = OpenAIPrompt().setPostProcessingOptions({"delimiter": ";"})
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "postProcessing must be 'csv'",
        ):
            csv_source.copy({csv_source.postProcessing: ""})

        self.assertEqual(csv_source.getPostProcessing(), "csv")
        self.assertEqual(
            csv_source.getPostProcessingOptions(),
            {"delimiter": ";"},
        )

        options_copy = source.copy({source.postProcessingOptions: {"delimiter": ";"}})
        self.assertEqual(options_copy.getPostProcessing(), "csv")
        self.assertEqual(
            options_copy.getPostProcessingOptions(),
            {"delimiter": ";"},
        )

        with self.assertRaisesRegex(IllegalArgumentException, "Invalid delimiter"):
            source.copy({source.postProcessingOptions: {"delimiter": "["}})

    def test_set_params_converts_all_values_before_mutating(self):
        prompt = OpenAIPrompt().setPostProcessing("csv")

        with self.assertRaises(TypeError):
            prompt.setParams(
                postProcessing="json",
                concurrency="not-an-integer",
            )

        self.assertEqual(prompt.getPostProcessing(), "csv")
        self.assertEqual(prompt.getConcurrency(), 1)

    def test_constructor_infers_modes_and_rejects_conflicts(self):
        inference_cases = [
            ({"delimiter": ";"}, "csv"),
            ({"jsonSchema": "value STRING"}, "json"),
            ({"regex": "(.*)", "regexGroup": "1"}, "regex"),
        ]

        for options, expected_mode in inference_cases:
            with self.subTest(options=options):
                prompt = OpenAIPrompt(postProcessingOptions=options)
                self.assertEqual(prompt.getPostProcessing(), expected_mode)
                self.assertEqual(prompt.getPostProcessingOptions(), options)

        cases = [
            {
                "postProcessingOptions": {"delimiter": ";"},
                "postProcessing": "json",
            },
            {
                "postProcessing": "json",
                "postProcessingOptions": {"delimiter": ";"},
            },
        ]

        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(
                    IllegalArgumentException,
                    "postProcessing must be 'csv'",
                ):
                    OpenAIPrompt(**kwargs)

        prompt = OpenAIPrompt(
            postProcessingOptions={"delimiter": ";"},
            postProcessing="csv",
        )
        self.assertEqual(prompt.getPostProcessing(), "csv")
        self.assertEqual(prompt.getPostProcessingOptions(), {"delimiter": ";"})

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "postProcessing must be 'csv'",
        ):
            OpenAIPrompt(
                postProcessingOptions={"delimiter": ";"},
                postProcessing="",
            )

    def test_set_post_processing_options_rejects_conflicting_explicit_modes(self):
        cases = [
            ("json", {"delimiter": ","}, "csv"),
            ("csv", {"jsonSchema": "value STRING"}, "json"),
            ("json", {"regex": ".*", "regexGroup": "0"}, "regex"),
        ]

        for explicit_mode, options, inferred_mode in cases:
            with self.subTest(
                explicit_mode=explicit_mode,
                inferred_mode=inferred_mode,
            ):
                prompt = OpenAIPrompt().setPostProcessing(explicit_mode)

                with self.assertRaisesRegex(
                    IllegalArgumentException,
                    f"postProcessing must be '{inferred_mode}'",
                ):
                    prompt.setPostProcessingOptions(options)

                self.assertEqual(prompt.getPostProcessing(), explicit_mode)
                self.assertEqual(prompt._java_obj.getPostProcessing(), "")


if __name__ == "__main__":
    unittest.main()
