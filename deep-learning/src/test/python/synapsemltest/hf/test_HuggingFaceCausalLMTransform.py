# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.

import unittest
from synapse.ml.hf import HuggingFaceCausalLM
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *
from synapse.ml.core import __spark_package_version__

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class HuggingFaceCausalLMTester(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HuggingFaceCausalLMTester, self).__init__(*args, **kwargs)
        self.transformer = (
            HuggingFaceCausalLM()
            .setModelName("Qwen/Qwen2.5-0.5B-Instruct")
            .setInputCol("messages")
            .setOutputCol("result")
            .setModelParam(max_new_tokens=10)
        )
        self.strDataFrame = (
            spark.createDataFrame(
                [
                    (
                        "positive",
                        "output a single word (without quotes) of positive or negative in lower case to reflect their sentiment: I like SynapseML",
                    ),
                ]
            )
            .toDF("gt", "messages")
            .repartition(1)
        )
        self.listDataFrame = (
            spark.createDataFrame(
                [
                    (
                        "positive",
                        [
                            {
                                "role": "system",
                                "content": "Your job is to detect the sentiment of user reviews. Given some text, output a single word (without quotes) of positive or negative to reflect their intent. Output only that single word in lower case: no explanations or complete sentences.",
                            },
                            {"role": "user", "content": "I like SynapseML"},
                        ],
                    ),
                ]
            )
            .toDF("gt", "messages")
            .repartition(1)
        )

    def _assert_output(self, transformer, input_df):
        transformed_df = transformer.transform(input_df).collect()
        gt_col_value = [row.gt for row in transformed_df]
        output_col_value = [row.result for row in transformed_df]
        input_col_value = [row.messages for row in transformed_df]
        for i in range(len(gt_col_value)):
            assert (
                gt_col_value[i].lower() == output_col_value[i].lower()
            ), f"model prediction {output_col_value[i]} does not match with ground truth {gt_col_value[i]}, input message is {input_col_value[i]}"

    def test_str_df(self):
        self._assert_output(self.transformer, self.strDataFrame)

    def test_list_df(self):
        self._assert_output(self.transformer, self.listDataFrame)


if __name__ == "__main__":
    result = unittest.main()
