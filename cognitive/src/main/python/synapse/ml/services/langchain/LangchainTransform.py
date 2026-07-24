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
    >>> transformer.transform(sentenceDataFrame)

The chain must be Spark-picklable. Modern OpenAI client objects contain
non-picklable HTTP state and cannot be captured directly by this transformer.

If the chain does not have memory, you can also save and load the
Langchain Transformer. The saving of chains with memory is currently
not supported in Langchain, so we can't save transformers with that
kind of chains
Example Usage:
    >>> transformer.save(path)
    >>> loaded_transformer = LangchainTransformer.load(path)
"""


import json
import pickle
from langchain_core.load import dumps, loads
from openai import OpenAIError
from pyspark import cloudpickle, keyword_only
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
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType
from typing import cast, Optional, TypeVar, Type
from synapse.ml.core.platform import running_on_synapse_internal
from synapse.ml.core.serialize._safe_import import secure_import_class

OPENAI_API_VERSION = None
RL = TypeVar("RL", bound="MLReadable")


def _validate_chain_for_spark(chain) -> None:
    if not hasattr(chain, "invoke") and not hasattr(chain, "run"):
        raise TypeError("LangChain value must define invoke() or run().")

    try:
        cloudpickle.dumps(chain)
    except (pickle.PicklingError, TypeError) as pickling_error:
        raise TypeError(
            "LangChain value must be Spark-picklable. Modern OpenAI clients "
            "contain non-picklable HTTP state and cannot be captured by "
            "LangchainTransformer."
        ) from pickling_error


def _chain_result_to_string(result) -> str:
    if isinstance(result, str):
        return str(result)
    if hasattr(result, "content"):
        return str(result.content)
    if isinstance(result, dict):
        for key in ("text", "output", "result"):
            if key in result:
                return str(result[key])
        return json.dumps(result, default=str)
    return str(result)


class LangchainTransformerParamsWriter(DefaultParamsWriter):
    @staticmethod
    def _chain_serializer(chain) -> Optional[str]:
        if getattr(chain, "memory", None) is not None:
            raise NotImplementedError(
                "Memory saving is not currently supported in Langchain. "
                "Therefore, it is not possible to save this LangchainTransformer object, "
                "as its chain contains memory."
            )
        try:
            return dumps(chain)
        except TypeError as e:
            raise NotImplementedError(
                "This LangChain Runnable cannot be serialized by langchain-core."
            ) from e

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
    def load(self, path: str) -> RL:
        metadata = LangchainTransformerParamsReader.loadMetadata(path, self.sc)
        py_type: Type[RL] = secure_import_class(metadata["class"])
        instance = py_type()
        cast("Params", instance)._resetUid(metadata["uid"])
        # deserialize the chain before setting Params
        serialized_chain = metadata["paramMap"]["chain"]
        metadata["paramMap"]["chain"] = loads(
            serialized_chain,
            allowed_objects="core",
            secrets_from_env=False,
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
        errorCol="errorCol",
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
        self.errorCol = Param(self, "errorCol", "column for error")
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
        if errorCol:
            kwargs["errorCol"] = errorCol

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
        errorCol="errorCol",
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
        return (
            self.getOrDefault(self.apiVersion)
            if self.isDefined(self.apiVersion)
            else None
        )

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

    def setErrorCol(self, value: str):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(errorCol=value)

    def getErrorCol(self):
        """
        Returns:
            str: The name of the error column
        """
        return self.getOrDefault(self.errorCol)

    def _transform(self, dataset):
        """
        do langchain transformation for the input column,
        and save the transformed values to the output column.
        """
        chain = self.getChain()
        _validate_chain_for_spark(chain)
        initialize_prerun = self.running_on_synapse_internal and not self.isSet(
            self.url
        )
        prerun_url = self.getUrl() if initialize_prerun else None

        # Define the schema for the output of the UDF
        schema = StructType(
            [
                StructField("result", StringType(), True),
                StructField("error_message", StringType(), True),
            ]
        )

        @udf(schema)
        def udfFunction(x):
            if initialize_prerun:
                from synapse.ml.fabric.prerun.openai_prerun import OpenAIPrerun

                OpenAIPrerun(api_base=prerun_url).init_personalized_session(None)

            try:
                if hasattr(chain, "invoke"):
                    result = chain.invoke(x)
                elif hasattr(chain, "run"):
                    result = chain.run(x)
                else:
                    raise TypeError("LangChain value must define invoke() or run().")
                result = _chain_result_to_string(result)
                error_message = ""
            except OpenAIError as e:
                result = ""
                error_message = f"OpenAI API returned an API Error: {e}"

            return result, error_message

        outCol = self.getOutputCol()
        errorCol = self.getErrorCol()
        inCol = dataset[self.getInputCol()]

        temp_col_name = "result_" + str(self.uid)

        return (
            dataset.withColumn(temp_col_name, udfFunction(inCol))
            .withColumn(outCol, col(f"{temp_col_name}.result"))
            .withColumn(errorCol, col(f"{temp_col_name}.error_message"))
            .drop(temp_col_name)
        )

    def write(self) -> LangchainTransformerParamsWriter:
        writer = LangchainTransformerParamsWriter(instance=self)
        # set the should overwriter in writer as True
        writer = writer.overwrite()
        return writer

    @classmethod
    def read(cls) -> "LangchainTransformerParamsReader[RL]":
        """Returns a LangchainTransformerParamsReader instance for this class."""
        return LangchainTransformerParamsReader(cls)
