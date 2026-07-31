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

OpenAI credentials, URL, and API version can be configured inline or through
OpenAIDefaults. Inline transformer values take precedence. Modern OpenAI clients
are serialized on the driver and reconstructed inside each Spark worker.

If the chain does not have memory, you can also save and load the
Langchain Transformer. The saving of chains with memory is currently
not supported in Langchain, so we can't save transformers with that
kind of chains
Example Usage:
    >>> transformer.save(path)
    >>> loaded_transformer = LangchainTransformer.load(path)
"""


import hashlib
import json
import pickle
from langchain_core.load import dumps
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


def _worker_chain_cache_key(
    serialized_chain: str,
    secrets_map,
    import_mappings,
    initialize_prerun: bool,
    prerun_url: Optional[str],
) -> str:
    hasher = hashlib.sha256(serialized_chain.encode("utf-8"))
    for secret_id, secret in sorted(secrets_map.items()):
        hasher.update(secret_id.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(secret.encode("utf-8"))
        hasher.update(b"\0")
    for source, target in sorted(import_mappings.items()):
        hasher.update(repr((source, target)).encode("utf-8"))
    hasher.update(str(initialize_prerun).encode("utf-8"))
    hasher.update((prerun_url or "").encode("utf-8"))
    return hasher.hexdigest()


class LangchainTransformerParamsWriter(DefaultParamsWriter):
    @staticmethod
    def _chain_serializer(chain) -> Optional[str]:
        from synapse.ml.services.langchain._LangchainSerialization import (
            prepare_serialized_chain,
            _PERSISTED_IMPORT_MAPPINGS,
        )

        if getattr(chain, "memory", None) is not None:
            raise NotImplementedError(
                "Memory saving is not currently supported in Langchain. "
                "Therefore, it is not possible to save this LangchainTransformer object, "
                "as its chain contains memory."
            )
        try:
            serialized_chain = dumps(chain)
        except TypeError as e:
            raise NotImplementedError(
                "This LangChain Runnable cannot be serialized by langchain-core."
            ) from e
        worker_config = prepare_serialized_chain(
            serialized_chain,
            {},
            None,
            None,
            None,
            sanitize_transport=True,
        )
        if worker_config is None:
            raise NotImplementedError(
                "This LangChain Runnable cannot be serialized by langchain-core."
            )
        unsupported_mappings = set(worker_config.additional_import_mappings) - set(
            _PERSISTED_IMPORT_MAPPINGS
        )
        if unsupported_mappings:
            raise NotImplementedError(
                "This LangChain Runnable cannot be serialized by langchain-core."
            )
        return worker_config.serialized_chain

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
        from synapse.ml.services.langchain._LangchainSerialization import (
            contains_openai_client,
            load_persisted_chain,
            prepare_serialized_chain,
        )

        metadata = LangchainTransformerParamsReader.loadMetadata(path, self.sc)
        py_type: Type[RL] = secure_import_class(metadata["class"])
        instance = py_type()
        cast("Params", instance)._resetUid(metadata["uid"])
        serialized_chain = metadata["paramMap"]["chain"]
        serialized_config = json.loads(serialized_chain)
        metadata["paramMap"] = metadata["paramMap"].copy()
        metadata["paramMap"].pop("chain")

        subscription_key = None
        url = None
        api_version = None
        if contains_openai_client(serialized_config):
            from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults

            defaults = OpenAIDefaults()
            saved_key = metadata["paramMap"].get("subscriptionKey")
            saved_url = metadata["paramMap"].get("url")
            global_url = defaults.get_URL()
            internal_url = (
                instance.getUrl()
                if instance.running_on_synapse_internal
                and instance.isDefined(instance.url)
                else None
            )
            uses_trusted_default_url = False
            if saved_key is not None:
                subscription_key = saved_key
                if saved_url is not None:
                    url = saved_url
                else:
                    url = global_url if global_url is not None else internal_url
                    uses_trusted_default_url = url is not None
            else:
                subscription_key = defaults.get_subscription_key()
                url = global_url if global_url is not None else internal_url
                uses_trusted_default_url = url is not None
            if uses_trusted_default_url:
                metadata["paramMap"].pop("url", None)
                metadata["defaultParamMap"] = metadata.get("defaultParamMap", {}).copy()
                metadata["defaultParamMap"].pop("url", None)
            api_version = (
                metadata["paramMap"].get("apiVersion") or defaults.get_api_version()
            )
            if subscription_key is None or url is None:
                raise ValueError(
                    "Loading a saved LangChain OpenAI client requires both a "
                    "subscription key and URL inline or through OpenAIDefaults."
                )

        LangchainTransformerParamsReader.getAndSetParams(instance, metadata)
        worker_config = prepare_serialized_chain(
            serialized_chain,
            {},
            subscription_key,
            url,
            api_version,
            sanitize_transport=True,
        )
        if worker_config is None:
            raise NotImplementedError(
                "This saved LangChain Runnable cannot be deserialized by "
                "langchain-core."
            )
        instance.setChain(load_persisted_chain(worker_config))
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

    def _get_effective_openai_settings(self):
        from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults

        defaults = OpenAIDefaults()
        # Keep both established SynapseML configuration paths when client
        # libraries change: inline values override OpenAIDefaults.
        subscription_key = (
            self.getSubscriptionKey()
            if self.isSet(self.subscriptionKey)
            else defaults.get_subscription_key()
        )
        if self.isSet(self.url):
            url = self.getUrl()
            has_configured_url = True
        else:
            global_url = defaults.get_URL()
            has_configured_url = global_url is not None
            url = (
                global_url
                if global_url is not None
                else self.getUrl()
                if self.isDefined(self.url)
                else None
            )
        api_version = (
            self.getApiVersion()
            if self.isSet(self.apiVersion)
            else defaults.get_api_version()
        )
        return subscription_key, url, api_version, has_configured_url

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
        from synapse.ml.services.langchain._LangchainSerialization import (
            _AZURE_API_KEY_ONLY_TOKEN,
            _PERSISTED_IMPORT_MAPPINGS,
            prepare_chain_for_worker,
        )

        chain = self.getChain()
        (
            subscription_key,
            url,
            api_version,
            has_configured_url,
        ) = self._get_effective_openai_settings()
        worker_config = prepare_chain_for_worker(
            chain,
            subscription_key,
            url,
            api_version,
        )
        if worker_config is None:
            _validate_chain_for_spark(chain)
            picklable_chain = chain
        else:
            picklable_chain = None

        worker_serialized_chain = (
            worker_config.serialized_chain if worker_config is not None else None
        )
        worker_secrets_map = (
            worker_config.secrets_map if worker_config is not None else None
        )
        worker_import_mappings = dict(_PERSISTED_IMPORT_MAPPINGS)
        if worker_config is not None:
            worker_import_mappings.update(worker_config.additional_import_mappings)
        azure_api_key_only_token = _AZURE_API_KEY_ONLY_TOKEN
        initialize_prerun = self.running_on_synapse_internal and not has_configured_url
        prerun_url = url if initialize_prerun else None
        worker_cache_key = (
            _worker_chain_cache_key(
                worker_serialized_chain,
                worker_secrets_map,
                worker_import_mappings,
                initialize_prerun,
                prerun_url,
            )
            if worker_serialized_chain is not None
            else None
        )
        worker_chain = None

        worker_runtime_fields = {
            "async_client",
            "client",
            "http_async_client",
            "http_client",
            "root_async_client",
            "root_client",
        }
        worker_openai_client_names = {
            "AzureChatOpenAI",
            "AzureOpenAI",
            "AzureOpenAIEmbeddings",
            "ChatOpenAI",
            "OpenAI",
            "OpenAIEmbeddings",
        }
        worker_azure_client_names = {
            "AzureChatOpenAI",
            "AzureOpenAI",
            "AzureOpenAIEmbeddings",
        }

        def walk_worker_chain_objects(chain):
            pending = [chain]
            visited = set()
            while pending:
                value = pending.pop()
                if value is None or isinstance(value, (str, bytes, int, float, bool)):
                    continue
                value_id = id(value)
                if value_id in visited:
                    continue
                visited.add(value_id)
                if isinstance(value, dict):
                    pending.extend(value.values())
                    continue
                if isinstance(value, (list, tuple, set)):
                    pending.extend(value)
                    continue
                if not type(value).__module__.startswith("langchain"):
                    continue

                yield value
                if type(value).__name__ not in worker_openai_client_names:
                    pending.extend(
                        item
                        for field_name, item in getattr(value, "__dict__", {}).items()
                        if field_name not in worker_runtime_fields
                    )

        def worker_sdk_clients(value):
            clients = (
                getattr(value, "root_client", None),
                getattr(value, "root_async_client", None),
                getattr(getattr(value, "client", None), "_client", None),
                getattr(getattr(value, "async_client", None), "_client", None),
            )
            seen = set()
            for client in clients:
                if client is not None and id(client) not in seen:
                    seen.add(id(client))
                    yield client

        def clear_worker_azure_ad_token_sentinel(chain):
            for value in walk_worker_chain_objects(chain):
                if type(value).__name__ not in worker_azure_client_names:
                    continue
                token = getattr(value, "azure_ad_token", None)
                if hasattr(token, "get_secret_value"):
                    token = token.get_secret_value()
                if token != azure_api_key_only_token:
                    continue

                value.azure_ad_token = None
                value.azure_ad_token_provider = None
                value.azure_ad_async_token_provider = None
                for client in worker_sdk_clients(value):
                    client._azure_ad_token = None
                    client._azure_ad_token_provider = None

        def worker_sync_sdk_clients(chain):
            clients = []
            seen = set()
            for value in walk_worker_chain_objects(chain):
                if type(value).__name__ in worker_openai_client_names:
                    candidates = (
                        getattr(value, "root_client", None),
                        getattr(getattr(value, "client", None), "_client", None),
                    )
                    for client in candidates:
                        if client is not None and id(client) not in seen:
                            seen.add(id(client))
                            clients.append(client)
            return clients

        def close_worker_clients(clients):
            for client in clients:
                close = getattr(client, "close", None)
                if callable(close):
                    close()

        def get_or_load_worker_chain():
            import builtins
            import weakref
            from collections import OrderedDict
            from langchain_core.load import loads

            cache_name = "_synapseml_langchain_worker_cache"
            cache = getattr(builtins, cache_name, None)
            if cache is None:
                cache = OrderedDict()
                setattr(builtins, cache_name, cache)

            if worker_cache_key in cache:
                cached_chain = cache.pop(worker_cache_key)
                cache[worker_cache_key] = cached_chain
                return cached_chain

            if initialize_prerun:
                from synapse.ml.fabric.prerun.openai_prerun import OpenAIPrerun

                OpenAIPrerun(api_base=prerun_url).init_personalized_session(None)
            loaded_chain = loads(
                worker_serialized_chain,
                allowed_objects="all",
                secrets_map=worker_secrets_map,
                valid_namespaces=["langchain_classic"],
                additional_import_mappings=worker_import_mappings,
                secrets_from_env=False,
            )
            clear_worker_azure_ad_token_sentinel(loaded_chain)
            clients = worker_sync_sdk_clients(loaded_chain)
            if clients:
                weakref.finalize(loaded_chain, close_worker_clients, clients)
            cache[worker_cache_key] = loaded_chain
            while len(cache) > 8:
                cache.popitem(last=False)
            return loaded_chain

        # Define the schema for the output of the UDF
        schema = StructType(
            [
                StructField("result", StringType(), True),
                StructField("error_message", StringType(), True),
            ]
        )

        @udf(schema)
        def udfFunction(x):
            nonlocal worker_chain

            if worker_chain is None:
                if worker_serialized_chain is not None:
                    worker_chain = get_or_load_worker_chain()
                else:
                    if initialize_prerun:
                        from synapse.ml.fabric.prerun.openai_prerun import OpenAIPrerun

                        OpenAIPrerun(api_base=prerun_url).init_personalized_session(
                            None
                        )
                    worker_chain = picklable_chain

            try:
                if hasattr(worker_chain, "invoke"):
                    result = worker_chain.invoke(x)
                elif hasattr(worker_chain, "run"):
                    result = worker_chain.run(x)
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
