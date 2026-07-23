# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import os
import subprocess
import unittest

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnableLambda
from langchain_openai import ChatOpenAI

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.langchain import LangchainTransformer

spark = init_spark()


def make_chain(llm):
    prompt = PromptTemplate.from_template(
        "Repeat the following word, just output the word again: {technology}"
    )
    return (
        RunnableLambda(lambda technology: {"technology": technology})
        | prompt
        | llm
        | StrOutputParser()
    )


class LangchainRunnableTest(unittest.TestCase):
    def test_transformer_invokes_runnable(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(RunnableLambda(lambda value: f"echo:{value}"))
        )
        result = transformer.transform(spark.createDataFrame([("test",)], ["value"]))
        self.assertEqual(result.select("result").first()[0], "echo:test")


class LangchainTransformTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        secret_json = subprocess.check_output(
            "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key-3",
            shell=True,
        )
        cls.subscription_key = json.loads(secret_json)["value"]
        cls.url = "https://synapseml-openai-3.openai.azure.com/openai/v1/"

    def setUp(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url=self.url,
            api_key=self.subscription_key,
            max_completion_tokens=500,
            reasoning_effort="low",
        )
        self.chain = make_chain(llm)
        self.langchainTransformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(self.chain)
            .setSubscriptionKey(self.subscription_key)
            .setUrl(self.url)
        )
        self.sentenceDataFrame = spark.createDataFrame(
            [(0, "docker"), (0, "spark"), (1, "python")], ["label", "technology"]
        )

    def _assert_chain_output(self, transformer, dataframe=None):
        if dataframe is None:
            dataframe = self.sentenceDataFrame
        transformed_df = transformer.transform(dataframe)
        collected_transformed_df = transformed_df.collect()
        input_col_values = [row.technology for row in collected_transformed_df]
        output_col_values = [row.copied_technology for row in collected_transformed_df]

        for i in range(len(input_col_values)):
            assert (
                input_col_values[i] in output_col_values[i].lower()
            ), f"output column value {output_col_values[i]} doesn't contain input column value {input_col_values[i]}"

    def test_langchainTransform(self):
        dataframes_to_test = spark.createDataFrame(
            [(0, "docker"), (0, "spark"), (1, "python")], ["label", "technology"]
        )
        self._assert_chain_output(self.langchainTransformer, dataframes_to_test)

    def test_langchainTransformErrorHandling(self):
        # Verify that OpenAI API errors are captured in errorCol rather than
        # crashing the Spark job.  We force a reliable InvalidRequestError by
        # setting max_completion_tokens=0 (below the API minimum of 1).
        error_llm = ChatOpenAI(
            model="gpt-5.1",
            base_url=self.url,
            api_key=self.subscription_key,
            max_completion_tokens=0,
            reasoning_effort="low",
        )
        error_chain = make_chain(error_llm)
        error_transformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(error_chain)
            .setSubscriptionKey(self.subscription_key)
            .setUrl(self.url)
        )

        dataframes_to_test = spark.createDataFrame(
            [(0, "hello")], ["label", "technology"]
        )
        transformed_df = error_transformer.transform(dataframes_to_test)
        collected = transformed_df.collect()
        error_col_values = [row.errorCol for row in collected]

        for error_val in error_col_values:
            assert (
                error_val and len(error_val) > 0
            ), "Expected an error message in errorCol but got empty/null"
            assert (
                "max_completion_tokens" in error_val
            ), f"Expected token validation error, got: {error_val}"

    def test_langchainTransformMiniModel(self):
        mini_llm = ChatOpenAI(
            model="gpt-5-mini",
            base_url=self.url,
            api_key=self.subscription_key,
            max_completion_tokens=500,
            reasoning_effort="low",
        )
        mini_chain = make_chain(mini_llm)
        mini_transformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(mini_chain)
            .setSubscriptionKey(self.subscription_key)
            .setUrl(self.url)
        )

        dataframes_to_test = spark.createDataFrame(
            [(0, "docker"), (0, "spark")], ["label", "technology"]
        )
        self._assert_chain_output(mini_transformer, dataframes_to_test)

    @unittest.skip("RunnableLambda serialization is not supported by langchain-core.")
    def test_save_load(self):
        dataframes_to_test = spark.createDataFrame(
            [(0, "docker"), (0, "spark"), (1, "python")], ["label", "technology"]
        )
        temp_dir = "tmp"
        os.makedirs(temp_dir, exist_ok=True)
        path = os.path.join(temp_dir, "langchainTransformer")
        self.langchainTransformer.save(path)
        loaded_transformer = LangchainTransformer.load(path)
        self._assert_chain_output(loaded_transformer, dataframes_to_test)


if __name__ == "__main__":
    result = unittest.main()
