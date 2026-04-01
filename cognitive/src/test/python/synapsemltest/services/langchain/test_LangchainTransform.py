# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# TODO: Upgrade to langchain>=0.3 + langchain-openai>=0.2 + openai>=1.0.
# The CompatAzureChatOpenAI subclass below works around langchain==0.0.152
# always sending max_tokens (rejected by reasoning models). Upgrading makes
# it unnecessary because langchain-openai 0.2+ natively uses
# max_completion_tokens and the openai 1.x SDK omits unset params.

import os, json, subprocess, unittest
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.chat_models import AzureChatOpenAI
from synapse.ml.services.langchain import LangchainTransformer
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class CompatAzureChatOpenAI(AzureChatOpenAI):
    """Strips the ``max_tokens`` parameter from API requests.

    Newer Azure OpenAI model deployments (e.g. gpt-4o) reject the legacy
    ``max_tokens`` field and require ``max_completion_tokens`` instead.
    LangChain <= 0.0.x always includes ``max_tokens`` in API params, even
    when unset (sent as ``null``). This subclass removes it so callers can
    pass ``max_completion_tokens`` via ``model_kwargs`` without conflict.
    """

    @property
    def _default_params(self):
        params = super()._default_params
        params.pop("max_tokens", None)
        return params


class LangchainTransformTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LangchainTransformTest, self).__init__(*args, **kwargs)
        # fetching openai_api_key
        secretJson = subprocess.check_output(
            "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key-2",
            shell=True,
        )
        openai_api_key = json.loads(secretJson)["value"]
        openai_api_base = "https://synapseml-openai-2.openai.azure.com/"
        openai_api_version = "2025-01-01-preview"
        openai_api_type = "azure"

        os.environ["OPENAI_API_TYPE"] = openai_api_type
        os.environ["OPENAI_API_VERSION"] = openai_api_version
        os.environ["OPENAI_API_BASE"] = openai_api_base
        os.environ["OPENAI_API_KEY"] = openai_api_key

        self.subscriptionKey = openai_api_key
        self.url = openai_api_base

        self.copy_prompt = PromptTemplate(
            input_variables=["technology"],
            template="Repeat the following word, just output the word again: {technology}",
        )

        # construction of llm
        llm = CompatAzureChatOpenAI(
            api_version="2025-01-01-preview",
            deployment_name="gpt-4o",
            model_kwargs={"max_completion_tokens": 100},
            temperature=0,
            verbose=False,
        )

        self.chain = LLMChain(llm=llm, prompt=self.copy_prompt)
        self.langchainTransformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(self.chain)
            .setSubscriptionKey(self.subscriptionKey)
            .setUrl(self.url)
        )

        # construction of test dataframe
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
        # construct langchain transformer using the chain defined above. And test if the generated
        # column has the expected result.
        dataframes_to_test = spark.createDataFrame(
            [(0, "docker"), (0, "spark"), (1, "python")], ["label", "technology"]
        )
        self._assert_chain_output(self.langchainTransformer, dataframes_to_test)

    def test_langchainTransformErrorHandling(self):
        # Verify that OpenAI API errors are captured in errorCol rather than
        # crashing the Spark job.  We force a reliable InvalidRequestError by
        # setting max_completion_tokens=0 (below the API minimum of 1).
        error_llm = CompatAzureChatOpenAI(
            api_version="2025-01-01-preview",
            deployment_name="gpt-4o",
            model_kwargs={"max_completion_tokens": 0},
            temperature=0,
            verbose=False,
        )
        error_chain = LLMChain(llm=error_llm, prompt=self.copy_prompt)
        error_transformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(error_chain)
            .setSubscriptionKey(self.subscriptionKey)
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
                "invalid" in error_val.lower()
            ), f"Expected 'invalid' in error message, got: {error_val}"

    def test_langchainTransformReasoningModelErrorCapture(self):
        # Verify that using plain AzureChatOpenAI (without the compat
        # workaround) against a reasoning model deployment captures the
        # max_tokens rejection in errorCol rather than crashing the Spark job.
        # This is the error a customer would hit with langchain==0.0.152.
        raw_llm = AzureChatOpenAI(
            api_version="2025-01-01-preview",
            deployment_name="gpt-4o",
            temperature=0,
            verbose=False,
        )
        raw_chain = LLMChain(llm=raw_llm, prompt=self.copy_prompt)
        raw_transformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(raw_chain)
            .setSubscriptionKey(self.subscriptionKey)
            .setUrl(self.url)
        )

        dataframes_to_test = spark.createDataFrame(
            [(0, "hello")], ["label", "technology"]
        )
        transformed_df = raw_transformer.transform(dataframes_to_test)
        collected = transformed_df.collect()
        error_col_values = [row.errorCol for row in collected]

        for error_val in error_col_values:
            assert (
                error_val and len(error_val) > 0
            ), "Expected an error in errorCol when using plain AzureChatOpenAI with reasoning model"

    def test_langchainTransformNonReasoningModel(self):
        # Verify that the compat workaround works with non-reasoning models too.
        # gpt-4.1-mini on synapseml-openai-2 is a non-reasoning model.
        non_reasoning_llm = CompatAzureChatOpenAI(
            api_version="2025-01-01-preview",
            deployment_name="gpt-4.1-mini",
            model_kwargs={"max_completion_tokens": 100},
            temperature=0,
            verbose=False,
        )
        non_reasoning_chain = LLMChain(llm=non_reasoning_llm, prompt=self.copy_prompt)
        non_reasoning_transformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(non_reasoning_chain)
            .setSubscriptionKey(self.subscriptionKey)
            .setUrl(self.url)
        )

        dataframes_to_test = spark.createDataFrame(
            [(0, "docker"), (0, "spark")], ["label", "technology"]
        )
        self._assert_chain_output(non_reasoning_transformer, dataframes_to_test)

    @unittest.skip(
        "Skipping this test because not supported for langchain.chat_models.AzureChatOpenAI."
    )
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
