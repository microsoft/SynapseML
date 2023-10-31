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
    >>> transformer = LangchainTransformer()
    ...                       .setInputCol("input_column_name")
    ...                       .setOutputCol("output_column_name")
    ...                       .setChain(pre_defined_chain)
    ...                       .setSubscriptionKey(OPENAI_API_KEY)
    ...                       .setUrl(baseURL)
    >>> transformer.transform(sentenceDataFrame)

If the chain does not have memory, you can also save and load the
Langchain Transformer. The saving of chains with memory is currently
not supported in Langchain, so we can't save transformers with that
kind of chains
Example Usage:
    >>> transformer.save(path)
    >>> loaded_transformer = LangchainTransformer.load(path)
"""

import json
from langchain.chains.loading import load_chain_from_config
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    Param,
)
from pyspark.ml.util import (
    DefaultParamsReadable,
    DefaultParamsWritable,
    DefaultParamsReader,
    DefaultParamsWriter,
)
from pyspark.sql.functions import udf
from typing import cast, Optional, TypeVar, Type
from synapse.ml.core.platform import running_on_synapse_internal

OPENAI_API_VERSION = "2022-12-01"
RL = TypeVar("RL", bound="MLReadable")


class LangchainTransformerParamsWriter(DefaultParamsWriter):
    @staticmethod
    def _chain_serializer(chain) -> Optional[str]:
        if chain.memory is not None:
            raise NotImplementedError(
                "Memory saving is not currently supported in Langchain. "
                "Therefore, it is not possible to save this LangchainTransformer object, "
                "as its chain contains memory."
            )
        return json.dumps(chain.dict())

    def saveImpl(self, path: str) -> None:
        params = self.instance._paramMap
        modifiedParamMap = {}
        for p in params:
            if "chain" in p.name:
                # For parameter chain, we need to first
                # serialize it, and then pass it to
                # parameter map.
                param_value = self._chain_serializer(params[p])
            else:
                param_value = params[p]
            modifiedParamMap[p.name] = param_value
        DefaultParamsWriter.saveMetadata(
            self.instance, path, self.sc, paramMap=modifiedParamMap
        )


class LangchainTransformerParamsReader(DefaultParamsReader):
    @staticmethod
    def __get_class(clazz: str) -> Type[RL]:
        """
        Loads Python class from its name.
        """
        parts = clazz.split(".")
        module = ".".join(parts[:-1])
        m = __import__(module, fromlist=[parts[-1]])
        return getattr(m, parts[-1])

    def load(self, path: str) -> RL:
        metadata = LangchainTransformerParamsReader.loadMetadata(path, self.sc)
        py_type: Type[RL] = LangchainTransformerParamsReader.__get_class(
            metadata["class"]
        )
        instance = py_type()
        cast("Params", instance)._resetUid(metadata["uid"])
        # deserialize the chain before setting Params
        metadata["paramMap"]["chain"] = load_chain_from_config(
            json.loads(metadata["paramMap"]["chain"])
        )
        LangchainTransformerParamsReader.getAndSetParams(instance, metadata)
        return instance


class LangchainTransformer(
    Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable
):
    @keyword_only
    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        chain=None,
        subscriptionKey=None,
        url=None,
        apiVersion=OPENAI_API_VERSION,
    ):
        super(LangchainTransformer, self).__init__()
        self.chain = Param(
            self,
            "chain",
            "Langchain chain",
        )
        self.subscriptionKey = Param(self, "subscriptionKey", "openai api key")
        self.url = Param(self, "url", "openai api base")
        self.apiVersion = Param(self, "apiVersion", "openai api version")
        self.running_on_synapse_internal = running_on_synapse_internal()
        if running_on_synapse_internal():
            from synapse.ml.fabric.service_discovery import get_fabric_env_config

            self._setDefault(
                url=get_fabric_env_config().fabric_env_config.ml_workload_endpoint
                + "cognitive/openai"
            )
        kwargs = self._input_kwargs
        if subscriptionKey:
            kwargs["subscriptionKey"] = subscriptionKey
        if url:
            kwargs["url"] = url
        if apiVersion:
            kwargs["apiVersion"] = apiVersion
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol=None,
        outputCol=None,
        chain=None,
        subscriptionKey=None,
        url=None,
        apiVersion=OPENAI_API_VERSION,
    ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setChain(self, value):
        return self._set(chain=value)

    def getChain(self):
        return self.getOrDefault(self.chain)

    def setSubscriptionKey(self, value: str):
        """
        set the openAI api key
        """
        return self._set(subscriptionKey=value)

    def getSubscriptionKey(self):
        return self.getOrDefault(self.subscriptionKey)

    def setUrl(self, value: str):
        return self._set(url=value)

    def getUrl(self):
        return self.getOrDefault(self.url)

    def setApiVersion(self, value: str):
        return self._set(apiVersion=value)

    def getApiVersion(self):
        return self.getOrDefault(self.apiVersion)

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

        @udf
        def udfFunction(x):
            import openai

            if self.running_on_synapse_internal and not self.isSet(self.url):
                from synapse.ml.fabric.prerun.openai_prerun import OpenAIPrerun

                OpenAIPrerun(api_base=self.getUrl()).init_personalized_session(None)
            else:
                openai.api_type = "azure"
                openai.api_key = self.getSubscriptionKey()
                openai.api_base = self.getUrl()
                openai.api_version = self.getApiVersion()
            return self.getChain().run(x)

        outCol = self.getOutputCol()
        inCol = dataset[self.getInputCol()]
        return dataset.withColumn(outCol, udfFunction(inCol))

    def write(self) -> LangchainTransformerParamsWriter:
        writer = LangchainTransformerParamsWriter(instance=self)
        # set the should overwriter in writer as True
        writer = writer.overwrite()
        return writer

    @classmethod
    def read(cls) -> "LangchainTransformerParamsReader[RL]":
        """Returns a LangchainTransformerParamsReader instance for this class."""
        return LangchainTransformerParamsReader(cls)
