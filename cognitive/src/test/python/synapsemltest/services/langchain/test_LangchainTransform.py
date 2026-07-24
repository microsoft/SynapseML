# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import tempfile
import unittest
from pathlib import Path

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langchain_openai import ChatOpenAI
from openai import OpenAIError

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.langchain import LangchainTransformer

spark = init_spark()


class LangchainRunnableTest(unittest.TestCase):
    def test_transformer_invokes_runnable(self):
        class TextAccessor(str):
            pass

        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(RunnableLambda(lambda value: TextAccessor(f"echo:{value}")))
        )

        row = transformer.transform(
            spark.createDataFrame([("test",)], ["value"])
        ).first()

        self.assertEqual(row.result, "echo:test")
        self.assertEqual(row.errorCol, "")

    def test_transformer_captures_modern_openai_error(self):
        def raise_openai_error(_):
            raise OpenAIError("modern OpenAI SDK error")

        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(RunnableLambda(raise_openai_error))
        )

        row = transformer.transform(
            spark.createDataFrame([("test",)], ["value"])
        ).first()

        self.assertEqual(row.result, "")
        self.assertIn("modern OpenAI SDK error", row.errorCol)

    def test_transformer_saves_serializable_runnable(self):
        chain = PromptTemplate.from_template("Define {value}")
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(chain)
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / "langchain-transformer")
            transformer.save(path)
            loaded_transformer = LangchainTransformer.load(path)

        self.assertIsInstance(loaded_transformer.getChain(), PromptTemplate)
        self.assertEqual(loaded_transformer.getInputCol(), "value")
        self.assertEqual(loaded_transformer.getOutputCol(), "result")

    def test_transformer_rejects_plain_callable(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(lambda value: value)
        )

        with self.assertRaisesRegex(TypeError, "must define invoke"):
            transformer.transform(spark.createDataFrame([("test",)], ["value"]))

    def test_transformer_rejects_non_picklable_openai_client(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://example.openai.azure.com/openai/v1/",
            api_key="test-key",
            max_retries=0,
        )
        try:
            prompt = PromptTemplate.from_template("{technology}")
            chain = (
                {"technology": RunnablePassthrough()} | prompt | llm | StrOutputParser()
            )
            transformer = (
                LangchainTransformer()
                .setInputCol("value")
                .setOutputCol("result")
                .setChain(chain)
            )

            with self.assertRaisesRegex(TypeError, "Spark-picklable"):
                transformer.transform(spark.createDataFrame([("test",)], ["value"]))
        finally:
            llm.root_client.close()


if __name__ == "__main__":
    unittest.main()
