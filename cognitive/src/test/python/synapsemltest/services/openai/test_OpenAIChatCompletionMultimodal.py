# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

from pyspark.sql import Row
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai.OpenAIChatCompletion import OpenAIChatCompletion

spark = init_spark()


class TestOpenAIChatCompletionMultimodal(unittest.TestCase):
    def test_empty_structured_content_is_a_row_error(self):
        image_url_schema = StructType(
            [
                StructField("url", StringType(), True),
                StructField("detail", StringType(), True),
            ]
        )
        content_part_schema = StructType(
            [
                StructField("type", StringType(), False),
                StructField("text", StringType(), True),
                StructField("image_url", image_url_schema, True),
            ]
        )
        message_schema = StructType(
            [
                StructField("role", StringType(), False),
                StructField("content", ArrayType(content_part_schema, True), True),
                StructField("name", StringType(), True),
            ]
        )
        schema = StructType(
            [
                StructField("messages", ArrayType(message_schema, True), True),
            ]
        )
        input_df = spark.createDataFrame(
            [Row(messages=[Row(role="user", content=[], name=None)])],
            schema,
        )
        transformer = (
            OpenAIChatCompletion()
            .setUrl("https://example.services.ai.azure.com/openai/v1")
            .setDeploymentName("gpt-5.1")
            .setMessagesCol("messages")
            .setSubscriptionKey("unused")
            .setOutputCol("output")
            .setErrorCol("error")
        )

        result = (
            transformer.transform(input_df).select("messages", "output", "error").head()
        )

        self.assertEqual(result.error.response, "messages[0].content must not be empty")
        self.assertIsNone(result.error.status)
        self.assertIsNone(result.output)
        self.assertEqual(result.messages[0].role, "user")
        self.assertEqual(result.messages[0].content, [])


if __name__ == "__main__":
    unittest.main()
