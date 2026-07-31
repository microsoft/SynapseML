# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

from pyspark.errors.exceptions.captured import IllegalArgumentException

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
                self.assertEqual(prompt._java_obj.getPostProcessing(), expected_mode)

    def test_set_post_processing_options_accepts_valid_regex(self):
        options = {"regex": "value=(.*)", "regexGroup": "1"}
        prompt = OpenAIPrompt().setPostProcessingOptions(options)

        self.assertEqual(prompt.getPostProcessing(), "regex")
        self.assertEqual(prompt.getPostProcessingOptions(), options)
        self.assertEqual(prompt._java_obj.getPostProcessing(), "regex")

    def test_set_post_processing_options_rejects_regex_without_group(self):
        prompt = OpenAIPrompt()

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "regexGroup must be specified with regex",
        ):
            prompt.setPostProcessingOptions({"regex": ".*"})

    def test_set_params_delegates_post_processing_options_to_java(self):
        prompt = OpenAIPrompt().setParams(
            postProcessingOptions={"delimiter": ";"},
        )

        self.assertEqual(prompt.getPostProcessing(), "csv")
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

    def test_constructor_infers_modes_and_rejects_conflicts(self):
        inference_cases = [
            ({"delimiter": ";"}, "csv"),
            ({"jsonSchema": "{}"}, "json"),
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
                self.assertEqual(prompt._java_obj.getPostProcessing(), explicit_mode)


if __name__ == "__main__":
    unittest.main()
