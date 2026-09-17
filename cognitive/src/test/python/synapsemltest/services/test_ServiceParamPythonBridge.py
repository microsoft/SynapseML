# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest
from unittest.mock import patch

from py4j.protocol import Py4JJavaError

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai.OpenAIEmbedding import OpenAIEmbedding
from synapse.ml.services.openai.OpenAIPrompt import OpenAIPrompt
from synapse.ml.services.text.TextSentiment import TextSentiment

spark = init_spark()


class TestServiceParamPythonBridge(unittest.TestCase):
    def test_transfer_does_not_inspect_every_unset_param(self):
        embedding = OpenAIEmbedding()
        java_obj = embedding._java_obj

        class CountingJavaObject:
            def __init__(self, delegate):
                self.delegate = delegate
                self.get_param_calls = 0

            def getParam(self, name):
                self.get_param_calls += 1
                return self.delegate.getParam(name)

            def __getattr__(self, name):
                return getattr(self.delegate, name)

        counting_java_obj = CountingJavaObject(java_obj)
        embedding._java_obj = counting_java_obj
        embedding._transfer_params_to_java()

        expected_default_transfers = sum(
            embedding.hasDefault(param) for param in embedding.params
        )
        self.assertEqual(counting_java_obj.get_param_calls, expected_default_transfers)

    def test_generated_accessors_support_scalar_and_column_values(self):
        embedding = OpenAIEmbedding(textCol="body")
        self.assertEqual(embedding.getTextCol(), "body")

        embedding.setDimensionsCol("embedding_size")
        self.assertEqual(embedding.getDimensionsCol(), "embedding_size")

        sentiment = TextSentiment().setText(["hello"])
        self.assertEqual(sentiment.getText(), ["hello"])

        headers = {"trace-id": "test"}
        embedding.setTelemHeaders(headers)
        self.assertEqual(embedding.getTelemHeaders(), headers)

    def test_set_params_dispatches_service_arguments_through_setters(self):
        embedding = OpenAIEmbedding().setParams(textCol="body")
        self.assertEqual(embedding.getTextCol(), "body")

        prompt = OpenAIPrompt().setParams(temperatureCol="sampling")
        self.assertEqual(prompt.getTemperatureCol(), "sampling")

        with self.assertRaisesRegex(
            ValueError,
            "Cannot set both 'text' and 'textCol' in the same call",
        ):
            OpenAIEmbedding().setParams(text="hello", textCol="body")

        embedding = OpenAIEmbedding(text=None, textCol="body")
        self.assertEqual(embedding.getTextCol(), "body")

        with self.assertRaisesRegex(
            TypeError, "Service parameter 'textCol' cannot be None"
        ):
            OpenAIEmbedding().setParams(textCol=None)

    def test_getters_preserve_unset_and_wrong_binding_errors(self):
        with self.assertRaises(Py4JJavaError):
            OpenAIEmbedding().getText()
        with self.assertRaises(Py4JJavaError):
            OpenAIEmbedding(text="hello").getTextCol()
        with self.assertRaises(Py4JJavaError):
            OpenAIEmbedding(textCol="body").getText()

    def test_java_transfer_does_not_restore_stale_service_values(self):
        source = OpenAIEmbedding(text="original")
        restored = OpenAIEmbedding._from_java(source._java_obj)

        self.assertFalse(restored.isSet(restored.text))
        restored.setText("updated")
        restored._transfer_params_to_java()

        self.assertEqual(restored.getText(), "updated")

    def test_copy_preserves_columns_and_consumes_scalar_extra_values(self):
        column_source = OpenAIEmbedding(textCol="body")
        column_copy = column_source.copy()
        self.assertEqual(column_copy.getTextCol(), "body")

        scalar_copy = column_source.copy({column_source.text: "updated"})
        self.assertEqual(scalar_copy.getText(), "updated")
        self.assertFalse(scalar_copy.isSet(scalar_copy.text))

        restored = OpenAIEmbedding._from_java(column_source._java_obj)
        restored_copy = restored.copy()
        self.assertEqual(restored_copy.getTextCol(), "body")

        restored_copy.setText("scalar")
        self.assertEqual(restored_copy.getText(), "scalar")
        with self.assertRaises(Py4JJavaError):
            restored_copy.getTextCol()

    def test_transform_extra_service_value_uses_public_pyspark_path(self):
        embedding = OpenAIEmbedding(textCol="body")

        with patch.object(
            OpenAIEmbedding,
            "_transform",
            lambda copied, dataset: copied.getText(),
        ):
            result = embedding.transform(
                spark.range(0),
                {embedding.text: "updated"},
            )

        self.assertEqual(result, "updated")
        self.assertEqual(embedding.getTextCol(), "body")

    def test_openai_prompt_service_updates_remain_atomic(self):
        prompt = OpenAIPrompt().setTemperature(0.25)

        with self.assertRaises(TypeError):
            prompt.setParams(
                temperatureCol="sampling",
                concurrency="not-an-integer",
            )

        self.assertEqual(prompt.getTemperature(), 0.25)

    def test_openai_prompt_validates_service_updates_on_scratch_copy(self):
        prompt = OpenAIPrompt().setTemperature(0.25)
        original_java_id = prompt._java_obj._target_id
        setter_java_ids = []
        set_temperature_col = prompt.setTemperatureCol

        def record_setter(value):
            setter_java_ids.append(prompt._java_obj._target_id)
            return set_temperature_col(value)

        prompt.setTemperatureCol = record_setter
        prompt.setParams(temperatureCol="sampling")

        self.assertEqual(len(setter_java_ids), 2)
        self.assertNotEqual(setter_java_ids[0], original_java_id)
        self.assertEqual(setter_java_ids[1], original_java_id)
        self.assertEqual(prompt.getTemperatureCol(), "sampling")

    def test_ordinary_params_keep_python_param_behavior(self):
        embedding = OpenAIEmbedding().setParams(outputCol="vector")

        self.assertTrue(embedding.isSet(embedding.outputCol))
        self.assertEqual(embedding.getOutputCol(), "vector")


if __name__ == "__main__":
    unittest.main()
