# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""
This file defines the Langchain transformation in SynapseML.
Set a chain, input column, and output column on LangchainTransformer.
The transformer applies that chain to each input and writes the result.
Example Usage:
    >>> transformer = LangchainTransformer()
    ...                       .setInputCol("input_column_name")
    ...                       .setOutputCol("output_column_name")
    ...                       .setChain(pre_defined_chain)
    ...                       .setSubscriptionKey(OPENAI_API_KEY)
    ...                       .setUrl(baseURL)
    >>> transformer.transform(sentenceDataFrame)

OpenAI credentials, URL, and API version can be configured inline or through
OpenAIDefaults. Inline transformer values take precedence. Modern LangChain
OpenAI clients are reconstructed inside each Spark worker because their HTTP
state is not Spark-picklable.

If the chain does not have memory, you can also save and load the
Langchain Transformer. The saving of chains with memory is currently
not supported in Langchain, so we can't save transformers with that
kind of chains
Example Usage:
    >>> transformer.save(path)
    >>> loaded_transformer = LangchainTransformer.load(path)
"""


import atexit
import hashlib
import json
import pickle
from collections import OrderedDict
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
from typing import Dict, List, NamedTuple, Optional, Set, Tuple, Type, TypeVar, cast
from urllib.parse import SplitResult, urlsplit, urlunsplit
from synapse.ml.core.platform import running_on_synapse_internal
from synapse.ml.core.serialize._safe_import import secure_import_class

OPENAI_API_VERSION = None
RL = TypeVar("RL", bound="MLReadable")
_OPENAI_CLIENT_NAMES = {
    "AzureChatOpenAI",
    "AzureOpenAI",
    "AzureOpenAIEmbeddings",
    "ChatOpenAI",
    "OpenAI",
    "OpenAIEmbeddings",
}
_AZURE_OPENAI_CLIENT_NAMES = {
    "AzureChatOpenAI",
    "AzureOpenAI",
    "AzureOpenAIEmbeddings",
}
_OPENAI_KEY_SECRET_ID = "OPENAI_API_KEY"
_AZURE_OPENAI_KEY_SECRET_ID = "AZURE_OPENAI_API_KEY"
_AZURE_OPENAI_AD_TOKEN_SECRET_ID = "AZURE_OPENAI_AD_TOKEN"
_LANGCHAIN_RUNTIME_FIELDS = {
    "async_client",
    "client",
    "http_async_client",
    "http_client",
    "root_async_client",
    "root_client",
}
_MAX_CACHED_WORKER_CHAINS = 8
_WORKER_CHAIN_CACHE = OrderedDict()
_UNTRUSTED_PERSISTED_TRANSPORT_FIELDS = {
    "api_key",
    "api_version",
    "async_client",
    "azure_ad_async_token_provider",
    "azure_ad_token_provider",
    "base_url",
    "client",
    "default_headers",
    "default_query",
    "http_async_client",
    "http_client",
    "organization",
    "openai_organization",
    "openai_proxy",
    "root_async_client",
    "root_client",
}
_PERSISTED_PARTNER_IMPORT_MAPPINGS = {
    ("langchain", "chat_models", "openai", "ChatOpenAI",): (
        "langchain_openai",
        "chat_models",
        "base",
        "ChatOpenAI",
    ),
    ("langchain", "chat_models", "azure_openai", "AzureChatOpenAI",): (
        "langchain_openai",
        "chat_models",
        "azure",
        "AzureChatOpenAI",
    ),
    ("langchain", "llms", "openai", "OpenAI",): (
        "langchain_openai",
        "llms",
        "base",
        "OpenAI",
    ),
    ("langchain", "llms", "openai", "AzureOpenAI",): (
        "langchain_openai",
        "llms",
        "azure",
        "AzureOpenAI",
    ),
    ("langchain_classic", "chains", "llm", "LLMChain",): (
        "langchain_classic",
        "chains",
        "llm",
        "LLMChain",
    ),
}


class _WorkerChainConfig(NamedTuple):
    serialized_chain: str
    secrets_map: Dict[str, str]
    valid_namespaces: List[str]
    additional_import_mappings: Dict[Tuple[str, ...], Tuple[str, ...]]


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


def _secret_value(value) -> Optional[str]:
    if hasattr(value, "get_secret_value"):
        value = value.get_secret_value()
    return value if isinstance(value, str) and value else None


def _validated_service_url(url: str) -> SplitResult:
    parsed_url = urlsplit(url)
    if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
        raise ValueError("OpenAI URL must be an absolute HTTP or HTTPS URL.")
    if parsed_url.query or parsed_url.fragment:
        raise ValueError(
            "OpenAI URL must not contain a query string or fragment. "
            "Set the API version with setApiVersion instead."
        )
    return parsed_url


def _strip_azure_openai_path(path: str) -> str:
    # Canonicalize only recognized Azure resource hosts. Removing the existing
    # API suffix first prevents invalid paths such as /openai/openai/v1, while
    # non-Azure proxy and custom endpoint paths remain untouched.
    openai_path_index = path.find("/openai")
    if openai_path_index < 0:
        return path

    suffix_index = openai_path_index + len("/openai")
    if suffix_index == len(path) or path[suffix_index] == "/":
        return path[:openai_path_index]
    return path


def _openai_v1_base_url(url: str) -> str:
    parsed_url = _validated_service_url(url)
    hostname = parsed_url.hostname or ""
    path = parsed_url.path.rstrip("/")
    if hostname.endswith(
        (".openai.azure.com", ".services.ai.azure.com")
    ) and not path.endswith("/openai/v1"):
        path = _strip_azure_openai_path(path)
        path += "/openai/v1"
    return urlunsplit(parsed_url._replace(path=path + "/"))


def _azure_openai_resource_url(url: str) -> str:
    parsed_url = _validated_service_url(url)
    hostname = parsed_url.hostname or ""
    path = parsed_url.path.rstrip("/")
    if hostname.endswith((".openai.azure.com", ".services.ai.azure.com")):
        path = _strip_azure_openai_path(path)
    return urlunsplit(parsed_url._replace(path=path + "/"))


def _walk_chain_objects(chain):
    pending = [chain]
    visited: Set[int] = set()

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

        if type(value).__name__ in _OPENAI_CLIENT_NAMES:
            continue

        attributes = getattr(value, "__dict__", {})
        pending.extend(
            attribute
            for field_name, attribute in attributes.items()
            if field_name not in _LANGCHAIN_RUNTIME_FIELDS
        )


def _collect_chain_secrets(
    chain,
    ignored_secret_ids: Optional[Set[str]] = None,
) -> Dict[str, str]:
    secrets = {}
    ignored_secret_ids = ignored_secret_ids or set()

    for value in _walk_chain_objects(chain):
        for field_name, secret_id in getattr(value, "lc_secrets", {}).items():
            if secret_id in ignored_secret_ids:
                continue
            secret = _secret_value(getattr(value, field_name, None))
            if secret is not None:
                existing_secret = secrets.get(secret_id)
                if existing_secret is not None and existing_secret != secret:
                    raise ValueError(
                        "LangChain contains multiple values for secret "
                        f"{secret_id}. Set a single SynapseML subscription key "
                        "inline or through OpenAIDefaults before transforming."
                    )
                secrets[secret_id] = secret

    return secrets


def _close_chain_clients(chain) -> None:
    for value in _walk_chain_objects(chain):
        if type(value).__name__ in _OPENAI_CLIENT_NAMES:
            root_client = getattr(value, "root_client", None)
            close = getattr(root_client, "close", None)
            if callable(close):
                close()


def _clear_worker_chain_cache() -> None:
    while _WORKER_CHAIN_CACHE:
        _, chain = _WORKER_CHAIN_CACHE.popitem(last=False)
        _close_chain_clients(chain)


atexit.register(_clear_worker_chain_cache)


def _configure_serialized_chain(
    value,
    subscription_key: Optional[str],
    url: Optional[str],
    api_version: Optional[str],
    additional_import_mappings: Dict[Tuple[str, ...], Tuple[str, ...]],
    sanitize_transport: bool = False,
) -> None:
    if isinstance(value, dict):
        identifier = value.get("id")
        if (
            value.get("lc") == 1
            and value.get("type") == "constructor"
            and isinstance(identifier, list)
            and identifier
        ):
            identifier_tuple = tuple(identifier)
            class_name = identifier[-1]
            if identifier[0] == "langchain_classic":
                additional_import_mappings[identifier_tuple] = identifier_tuple

            if class_name in _OPENAI_CLIENT_NAMES:
                kwargs = value.setdefault("kwargs", {})
                is_azure_client = class_name in _AZURE_OPENAI_CLIENT_NAMES

                if sanitize_transport:
                    for field_name in _UNTRUSTED_PERSISTED_TRANSPORT_FIELDS:
                        kwargs.pop(field_name, None)

                if subscription_key is not None:
                    secret_id = (
                        _AZURE_OPENAI_KEY_SECRET_ID
                        if is_azure_client
                        else _OPENAI_KEY_SECRET_ID
                    )
                    kwargs["openai_api_key"] = {
                        "lc": 1,
                        "type": "secret",
                        "id": [secret_id],
                    }
                    if is_azure_client:
                        kwargs["azure_ad_token"] = None

                if url is not None:
                    if is_azure_client:
                        kwargs["azure_endpoint"] = _azure_openai_resource_url(url)
                        kwargs.pop("openai_api_base", None)
                    else:
                        kwargs["openai_api_base"] = _openai_v1_base_url(url)

                if api_version is not None and is_azure_client:
                    kwargs["openai_api_version"] = api_version

        for item in value.values():
            _configure_serialized_chain(
                item,
                subscription_key,
                url,
                api_version,
                additional_import_mappings,
                sanitize_transport,
            )
    elif isinstance(value, list):
        for item in value:
            _configure_serialized_chain(
                item,
                subscription_key,
                url,
                api_version,
                additional_import_mappings,
                sanitize_transport,
            )


def _prepare_chain_for_worker(
    chain,
    subscription_key: Optional[str],
    url: Optional[str],
    api_version: Optional[str],
) -> Optional[_WorkerChainConfig]:
    try:
        serialized_chain = dumps(chain)
    except TypeError:
        return None

    ignored_secret_ids = (
        {
            _OPENAI_KEY_SECRET_ID,
            _AZURE_OPENAI_KEY_SECRET_ID,
            _AZURE_OPENAI_AD_TOKEN_SECRET_ID,
        }
        if subscription_key is not None
        else set()
    )
    secrets_map = _collect_chain_secrets(chain, ignored_secret_ids)
    return _prepare_serialized_chain_for_worker(
        serialized_chain,
        secrets_map,
        subscription_key,
        url,
        api_version,
    )


def _prepare_serialized_chain_for_worker(
    serialized_chain: str,
    secrets_map: Dict[str, str],
    subscription_key: Optional[str],
    url: Optional[str],
    api_version: Optional[str],
    sanitize_transport: bool = False,
) -> Optional[_WorkerChainConfig]:
    serialized_config = json.loads(serialized_chain)
    if _contains_not_implemented(serialized_config):
        return None

    secrets_map = secrets_map.copy()
    if subscription_key is not None:
        secrets_map[_OPENAI_KEY_SECRET_ID] = subscription_key
        secrets_map[_AZURE_OPENAI_KEY_SECRET_ID] = subscription_key
        secrets_map.pop(_AZURE_OPENAI_AD_TOKEN_SECRET_ID, None)

    additional_import_mappings = {}
    _configure_serialized_chain(
        serialized_config,
        subscription_key,
        url,
        api_version,
        additional_import_mappings,
        sanitize_transport,
    )

    valid_namespaces = ["langchain_classic"] if additional_import_mappings else []
    return _WorkerChainConfig(
        json.dumps(serialized_config),
        secrets_map,
        valid_namespaces,
        additional_import_mappings,
    )


def _load_chain_for_worker(config: _WorkerChainConfig):
    # This manifest is generated from the caller's in-memory chain on the driver,
    # not loaded from an untrusted artifact. Partner integrations are therefore
    # allowed while environment-based secret loading remains disabled.
    return loads(
        config.serialized_chain,
        allowed_objects="all",
        secrets_map=config.secrets_map,
        valid_namespaces=config.valid_namespaces or None,
        additional_import_mappings=config.additional_import_mappings or None,
        secrets_from_env=False,
    )


def _load_persisted_chain(config: _WorkerChainConfig):
    return loads(
        config.serialized_chain,
        allowed_objects="core",
        secrets_map=config.secrets_map,
        valid_namespaces=["langchain_classic"],
        additional_import_mappings=_PERSISTED_PARTNER_IMPORT_MAPPINGS,
        secrets_from_env=False,
    )


def _get_or_load_chain_for_worker(config: _WorkerChainConfig):
    cache_key_hasher = hashlib.sha256()
    cache_key_hasher.update(config.serialized_chain.encode("utf-8"))
    for secret_id, secret in sorted(config.secrets_map.items()):
        cache_key_hasher.update(secret_id.encode("utf-8"))
        cache_key_hasher.update(b"\0")
        cache_key_hasher.update(secret.encode("utf-8"))
        cache_key_hasher.update(b"\0")
    cache_key = cache_key_hasher.hexdigest()

    cached_chain = _WORKER_CHAIN_CACHE.get(cache_key)
    if cached_chain is not None:
        _WORKER_CHAIN_CACHE.move_to_end(cache_key)
        return cached_chain

    chain = _load_chain_for_worker(config)
    _WORKER_CHAIN_CACHE[cache_key] = chain
    if len(_WORKER_CHAIN_CACHE) > _MAX_CACHED_WORKER_CHAINS:
        _, evicted_chain = _WORKER_CHAIN_CACHE.popitem(last=False)
        _close_chain_clients(evicted_chain)
    return chain


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


def _contains_not_implemented(value) -> bool:
    if isinstance(value, dict):
        if value.get("lc") == 1 and value.get("type") == "not_implemented":
            return True
        return any(_contains_not_implemented(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_not_implemented(item) for item in value)
    return False


def _contains_openai_client(value) -> bool:
    if isinstance(value, dict):
        identifier = value.get("id")
        if (
            value.get("lc") == 1
            and value.get("type") == "constructor"
            and isinstance(identifier, list)
            and identifier
            and identifier[-1] in _OPENAI_CLIENT_NAMES
        ):
            return True
        return any(_contains_openai_client(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_openai_client(item) for item in value)
    return False


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
            serialized_chain = dumps(chain)
        except TypeError as e:
            raise NotImplementedError(
                "This LangChain Runnable cannot be serialized by langchain-core."
            ) from e
        if _contains_not_implemented(json.loads(serialized_chain)):
            raise NotImplementedError(
                "This LangChain Runnable cannot be serialized by langchain-core."
            )
        return serialized_chain

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
        serialized_chain = metadata["paramMap"]["chain"]
        serialized_config = json.loads(serialized_chain)
        has_openai_client = _contains_openai_client(serialized_config)

        from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults

        defaults = OpenAIDefaults()
        saved_subscription_key = metadata["paramMap"].get("subscriptionKey")
        saved_url = metadata["paramMap"].get("url")
        saved_api_version = metadata["paramMap"].get("apiVersion")
        global_subscription_key = defaults.get_subscription_key()
        global_url = defaults.get_URL()
        global_api_version = defaults.get_api_version()

        use_global_credentials = (
            has_openai_client
            and saved_subscription_key is None
            and global_subscription_key is not None
        )
        if use_global_credentials:
            trusted_default_url = (
                global_url
                if global_url is not None
                else instance.getUrl()
                if instance.running_on_synapse_internal
                and instance.isDefined(instance.url)
                else None
            )
            if trusted_default_url is None:
                raise ValueError(
                    "Loading a saved LangChain OpenAI client with an "
                    "OpenAIDefaults subscription key also requires "
                    "OpenAIDefaults.set_URL()."
                )
            subscription_key = global_subscription_key
            url = trusted_default_url
            api_version = (
                saved_api_version
                if saved_api_version is not None
                else global_api_version
            )
        elif has_openai_client and saved_subscription_key is not None:
            subscription_key = saved_subscription_key
            url = saved_url if saved_url is not None else global_url
            api_version = (
                saved_api_version
                if saved_api_version is not None
                else global_api_version
            )
        elif has_openai_client:
            raise ValueError(
                "Loading a saved LangChain OpenAI client requires an inline "
                "subscription key in the saved transformer or OpenAIDefaults "
                "to be configured before load()."
            )
        else:
            subscription_key = None
            url = None
            api_version = None

        metadata_without_chain = metadata.copy()
        metadata_without_chain["paramMap"] = metadata["paramMap"].copy()
        metadata_without_chain["paramMap"].pop("chain")
        metadata_without_chain["defaultParamMap"] = metadata.get(
            "defaultParamMap", {}
        ).copy()
        if use_global_credentials:
            metadata_without_chain["paramMap"].pop("url", None)
            metadata_without_chain["defaultParamMap"].pop("url", None)
        LangchainTransformerParamsReader.getAndSetParams(
            instance, metadata_without_chain
        )

        worker_chain_config = _prepare_serialized_chain_for_worker(
            serialized_chain,
            {},
            subscription_key,
            url,
            api_version,
            sanitize_transport=True,
        )
        if worker_chain_config is None:
            raise NotImplementedError(
                "This saved LangChain Runnable cannot be deserialized by "
                "langchain-core."
            )

        instance.setChain(_load_persisted_chain(worker_chain_config))
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

    def _get_effective_openai_settings(
        self,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], bool]:
        from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults

        defaults = OpenAIDefaults()

        # Compatibility contract: explicit transformer parameters override
        # OpenAIDefaults, which fill only unset values. Client modernization must
        # preserve both configuration paths rather than relying on SDK globals.
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
        chain = self.getChain()
        (
            subscription_key,
            url,
            api_version,
            has_configured_url,
        ) = self._get_effective_openai_settings()
        worker_chain_config = _prepare_chain_for_worker(
            chain,
            subscription_key,
            url,
            api_version,
        )
        if worker_chain_config is None:
            _validate_chain_for_spark(chain)
            picklable_chain = chain
        else:
            picklable_chain = None

        initialize_prerun = self.running_on_synapse_internal and not has_configured_url
        prerun_url = url if initialize_prerun else None
        worker_chain = None

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
                if initialize_prerun:
                    from synapse.ml.fabric.prerun.openai_prerun import OpenAIPrerun

                    OpenAIPrerun(api_base=prerun_url).init_personalized_session(None)

                worker_chain = (
                    _get_or_load_chain_for_worker(worker_chain_config)
                    if worker_chain_config is not None
                    else picklable_chain
                )

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
