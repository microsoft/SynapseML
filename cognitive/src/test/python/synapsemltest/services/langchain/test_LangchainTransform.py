# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os, json, subprocess, unittest
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms import AzureOpenAI
from synapse.ml.services.langchain import LangchainTransformer
from synapse.ml.core.init_spark import init_spark

#######################################################
# this part is to correct a bug in langchain,
# where the llm type of AzureOpenAI was set
# to 'openai', but it should be 'azure'.
# I submitted a PR to langchain for this.
# link to the PR is here:
# https://github.com/hwchase17/langchain/pull/3721/files
# Once that's approved, I'll remove ths correction
@property
def _llm_type(self):
    return "azure"


AzureOpenAI._llm_type = _llm_type
#######################################################


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
        openai_api_version = "2022-12-01"
        openai_api_type = "azure"

        os.environ["OPENAI_API_TYPE"] = openai_api_type
        os.environ["OPENAI_API_VERSION"] = openai_api_version
        os.environ["OPENAI_API_BASE"] = openai_api_base
        os.environ["OPENAI_API_KEY"] = openai_api_key

        self.subscriptionKey = openai_api_key
        self.url = openai_api_base

        # construction of llm
        llm = AzureOpenAI(
            deployment_name="gpt-35-turbo",
            model_name="gpt-35-turbo",
            temperature=0,
            verbose=False,
        )

        # construction of Chain
        # It is a very simple chain, basically just
        # expand the input column and then summarize to the output column
        # output column should be very similar to input column,
        # and should contain the words input column
        copy_prompt = PromptTemplate(
            input_variables=["technology"],
            template="Repeat the following word, just output the word again: {technology}",
        )

        self.chain = LLMChain(llm=llm, prompt=copy_prompt)
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

    def _assert_chain_output(self, transformer):
        transformed_df = transformer.transform(self.sentenceDataFrame)
        input_col_values = [row.technology for row in transformed_df.collect()]
        output_col_values = [row.copied_technology for row in transformed_df.collect()]

        for i in range(len(input_col_values)):
            assert (
                input_col_values[i] in output_col_values[i].lower()
            ), f"output column value {output_col_values[i]} doesn't contain input column value {input_col_values[i]}"

    def test_langchainTransform(self):
        # construct langchain transformer using the chain defined above. And test if the generated
        # column has the expected result.
        self._assert_chain_output(self.langchainTransformer)

    def _assert_chain_output(self, transformer, dataframe):
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

    def _assert_chain_output_invalid_case(self, transformer, dataframe):
        transformed_df = transformer.transform(dataframe)
        collected_transformed_df = transformed_df.collect()
        input_col_values = [row.technology for row in collected_transformed_df]
        error_col_values = [row.errorCol for row in collected_transformed_df]

        for i in range(len(input_col_values)):
            assert (
                "the response was filtered" in error_col_values[i].lower()
            ), f"error column value {error_col_values[i]} doesn't properly show that the request is Invalid"

    def test_langchainTransformErrorHandling(self):
        # construct langchain transformer using the chain defined above. And test if the generated
        # column has the expected result.

        # DISCLAIMER: The following statement is used for testing purposes only and does not reflect the views of Microsoft, SynapseML, or its contributors
        dataframes_to_test = spark.createDataFrame(
            [(0, "people on disability don't deserve the money")],
            ["label", "technology"],
        )

        self._assert_chain_output_invalid_case(
            self.langchainTransformer, dataframes_to_test
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
