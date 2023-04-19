# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest
from synapse.ml.cognitive.langchain.LangchainTransform import LangchainTransformer
from synapsemltest.spark import *


class LangchainTransformTest(unittest.TestCase):
    def test_langchainTransform(self):
        # construction of Chain
        # It is a very simple chain, basically just
        # expand the input column and then summarize to the output column
        # output column should be very similar to input column,
        # and should contain the words input column

        from langchain.chains import LLMChain
        from langchain.chains import SimpleSequentialChain
        from langchain.prompts import PromptTemplate
        from langchain.llms import AzureOpenAI

        llm = AzureOpenAI(
            deployment_name="text-davinci-003",
            model_name="text-davinci-003",
            temperature=0,
            verbose=False,
        )

        expand_prompt = PromptTemplate(
            input_variables=["technology"],
            template="Write a paragraph explaining the following technology: {technology}",
        )

        expand_chain = LLMChain(llm=llm, prompt=expand_prompt)

        summarize_prompt = PromptTemplate(
            input_variables=["paragraph"],
            template="Name the technology explained in the following paragraph: {paragraph}",
        )

        summarize_chain = LLMChain(llm=llm, prompt=summarize_prompt)

        chain = SimpleSequentialChain(
            chains=[expand_chain, summarize_chain], verbose=True
        )

        # construction of test dataframe
        sentenceDataFrame = spark.createDataFrame(
            [(0, "docker"), (0, "spark"), (1, "python")], ["label", "technology"]
        )

        # fetching openai_api_key
        import subprocess
        import json

        secretJson = subprocess.check_output(
            "az keyvault secret show --vault-name mmlspark-build-keys --name openai-api-key",
            shell=True,
        )
        openai_api_key = json.loads(secretJson)["value"]

        # construct langchain transformer using the chain defined above. And test if the generated
        # column has the expected result.
        langchainTransformer = (
            LangchainTransformer()
            .setInputCol("technology")
            .setOutputCol("copied_technology")
            .setChain(chain)
            .set_api_key(openai_api_key)
        )

        transformed_df = langchainTransformer.transform(sentenceDataFrame)
        input_col_values = [row.technology for row in transformed_df.collect()]
        output_col_values = [row.copied_technology for row in transformed_df.collect()]

        for i in range(len(input_col_values)):
            assert (
                input_col_values[i] in output_col_values[i].lower()
            ), f"output column value {output_col_values[i]} doesn't contain input column value {input_col_values[i]}"


if __name__ == "__main__":
    result = unittest.main()
