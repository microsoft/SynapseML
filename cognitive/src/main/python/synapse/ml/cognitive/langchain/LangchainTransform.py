# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""
This file defines the Langchain transformation in SynapseML.
To use this transformation, one needs to first define a chain,
then set that chain as a parameter for the LangchainTransformer.
Also needs to specify the inputColumn and outputColumn.
Then this transformer will perform the operation defined in the
Langchain chain to transform the input Column and save it to the
OutputColumn.
Example Usage:
    >>> langchainTransformer = LangchainTransformer()
    ...                       .setInputCol("input_column_name")
    ...                       .setOutputCol("output_column_name")
    ...                       .setChain(pre_defined_chain)
    ...                       .set_api_key(OPENAI_API_KEY)
    >>> langchainTransformer.transform(sentenceDataFrame)

"""

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    Param,
    Params,
    TypeConverters,
)
from pyspark.sql.functions import udf

# Default Values
OPENAI_API_KEY = "64e8a259a46c496a9ccfb668895f78a9"
OPENAI_API_BASE = "https://scusopenai.openai.azure.com/"
OPENAI_API_VERSION = "2022-12-01"


class LangchainTransformer(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        chain=None,
        api_key=OPENAI_API_KEY,
        api_base=OPENAI_API_BASE,
        api_version=OPENAI_API_VERSION,
    ):
        super(LangchainTransformer, self).__init__()
        self.chain = Param(self, "chain", "Langchain chain")
        self.api_key = Param(self, "api_key", "openai api key")
        self.api_base = Param(self, "api_base", "openai api base")
        self.api_version = Param(self, "api_version", "openai api version")
        kwargs = self._input_kwargs
        if api_key:
            kwargs["api_key"] = api_key
        if api_base:
            kwargs["api_base"] = api_base
        if api_version:
            kwargs["api_version"] = api_version
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol=None,
        outputCol=None,
        chain=None,
        api_key=OPENAI_API_KEY,
        api_base=OPENAI_API_BASE,
        api_version=OPENAI_API_VERSION,
    ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setChain(self, value):
        return self._set(chain=value)

    def getChain(self):
        return self.getOrDefault(self.chain)

    def set_api_key(self, value: str):
        """
        set the openAI api key
        """
        return self._set(api_key=value)

    def get_api_key(self):
        return self.getOrDefault(self.api_key)

    def set_api_base(self, value: str):
        return self._set(api_base=value)

    def get_api_base(self):
        return self.getOrDefault(self.api_base)

    def set_api_version(self, value: str):
        return self._set(api_version=value)

    def get_api_version(self):
        return self.getOrDefault(self.api_version)

    def setInputCol(self, value: str):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _transform(self, dataset):
        """
        do langchain transformation for the input column,
        and save the transformed values to the output column.
        """
        def f(x):
            import openai
            openai.api_type = "azure"
            openai.api_key = self.get_api_key()
            openai.api_base = self.get_api_base()
            openai.api_version = self.get_api_version()
            return self.getChain().run(x)

        udf_function = udf(lambda x: f(x))

        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
        return dataset.withColumn(out_col, udf_function(in_col))
