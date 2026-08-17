# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from synapse.ml.services.openai.OpenAIResponses import OpenAIResponses

spark = SparkSession.builder.master("local[*]").appName("PysparkTests").getOrCreate()


class TestOpenAIResponsesMultimodal(unittest.TestCase):
    def test_invalid_structured_image_is_a_row_error(self):
        content_part_schema = StructType(
            [
                StructField("type", StringType(), False),
                StructField("text", StringType(), True),
                StructField("image_url", StringType(), True),
                StructField("detail", StringType(), True),
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
            [
                Row(
                    messages=[
                        Row(
                            role="user",
                            content=[
                                Row(
                                    type="input_image",
                                    text=None,
                                    image_url=None,
                                    detail="low",
                                )
                            ],
                            name=None,
                        )
                    ]
                )
            ],
            schema,
        )
        transformer = (
            OpenAIResponses()
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

        self.assertEqual(
            result.error.response,
            "messages[0].content[0] requires a non-empty string "
            "'image_url' or 'file_id' field",
        )
        self.assertIsNone(result.error.status)
        self.assertIsNone(result.output)
        self.assertEqual(result.messages[0].content[0].detail, "low")


if __name__ == "__main__":
    unittest.main()
