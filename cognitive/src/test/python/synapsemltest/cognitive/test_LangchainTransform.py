# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os, json, subprocess, unittest
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms import AzureOpenAI
from synapse.ml.cognitive.langchain.LangchainTransform import LangchainTransformer
from synapsemltest.spark import *


class LangchainTransformTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LangchainTransformTest, self).__init__(*args, **kwargs)
        # fetching openai_api_key
        secretJson = subprocess.check_output(
            "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key",
            shell=True,
        )
        openai_api_key = json.loads(secretJson)["value"]
        openai_api_base = "https://synapseml-openai.openai.azure.com/"
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
            deployment_name="text-davinci-003",
            model_name="text-davinci-003",
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
            template="Copy the following word: {technology}",
        )

        self.chain = LLMChain(llm=llm, prompt=copy_prompt)

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

        self.langchainTransformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(self.chain)
            .setSubscriptionKey(self.subscriptionKey)
            .setUrl(self.url)
        )

        self._assert_chain_output(self.langchainTransformer)

    def test_save_load(self):
        temp_dir = "tmp"
        os.mkdir(temp_dir)
        path = os.path.join(temp_dir, "langchainTransformer")
        self.langchainTransformer.save(path)
        loaded_transformer = LangchainTransformer.load(path)
        self._assert_chain_output(loaded_transformer)


if __name__ == "__main__":
    result = unittest.main()
