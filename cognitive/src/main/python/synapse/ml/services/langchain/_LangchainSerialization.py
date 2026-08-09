# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
from typing import Dict, NamedTuple, Optional, Set, Tuple
from urllib.parse import SplitResult, urlsplit, urlunsplit
from uuid import uuid4

from langchain_core.load import dumps, loads

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
_AZURE_API_KEY_ONLY_TOKEN = "SYNAPSEML_API_KEY_ONLY"
_OPENAI_SECRET_IDS = {
    _OPENAI_KEY_SECRET_ID,
    _AZURE_OPENAI_KEY_SECRET_ID,
    _AZURE_OPENAI_AD_TOKEN_SECRET_ID,
}
_RUNTIME_FIELDS = {
    "async_client",
    "client",
    "http_async_client",
    "http_client",
    "root_async_client",
    "root_client",
}
_UNTRUSTED_TRANSPORT_FIELDS = {
    "api_key",
    "api_version",
    "async_client",
    "azure_ad_async_token_provider",
    "azure_ad_token",
    "azure_ad_token_provider",
    "azure_endpoint",
    "base_url",
    "client",
    "default_headers",
    "default_query",
    "http_async_client",
    "http_client",
    "openai_api_base",
    "openai_api_key",
    "openai_organization",
    "openai_proxy",
    "organization",
    "root_async_client",
    "root_client",
}
_NESTED_UNTRUSTED_TRANSPORT_FIELDS = _UNTRUSTED_TRANSPORT_FIELDS | {
    "openai_api_version",
}
_PERSISTED_IMPORT_MAPPINGS = {
    ("langchain", "chat_models", "openai", "ChatOpenAI"): (
        "langchain_openai",
        "chat_models",
        "base",
        "ChatOpenAI",
    ),
    ("langchain", "chat_models", "azure_openai", "AzureChatOpenAI"): (
        "langchain_openai",
        "chat_models",
        "azure",
        "AzureChatOpenAI",
    ),
    ("langchain", "llms", "openai", "OpenAI"): (
        "langchain_openai",
        "llms",
        "base",
        "OpenAI",
    ),
    ("langchain", "llms", "openai", "AzureOpenAI"): (
        "langchain_openai",
        "llms",
        "azure",
        "AzureOpenAI",
    ),
    ("langchain_classic", "chains", "llm", "LLMChain"): (
        "langchain_classic",
        "chains",
        "llm",
        "LLMChain",
    ),
}


class WorkerChainConfig(NamedTuple):
    serialized_chain: str
    secrets_map: Dict[str, str]
    additional_import_mappings: Dict[Tuple[str, ...], Tuple[str, ...]]


def contains_not_implemented(value) -> bool:
    if isinstance(value, dict):
        if value.get("lc") == 1 and value.get("type") == "not_implemented":
            return True
        return any(contains_not_implemented(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_not_implemented(item) for item in value)
    return False


def contains_openai_client(value) -> bool:
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
        return any(contains_openai_client(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_openai_client(item) for item in value)
    return False


def _validate_secret_references(value, allowed_secret_ids: Set[str]) -> None:
    if isinstance(value, dict):
        if value.get("lc") == 1 and value.get("type") == "secret":
            identifier = value.get("id")
            if (
                not isinstance(identifier, list)
                or len(identifier) != 1
                or identifier[0] not in allowed_secret_ids
            ):
                raise ValueError(
                    "Saved LangChain artifacts cannot reference external secrets."
                )
        for item in value.values():
            _validate_secret_references(item, allowed_secret_ids)
    elif isinstance(value, list):
        for item in value:
            _validate_secret_references(item, allowed_secret_ids)


def _iter_langchain_objects(value, visited=None):
    visited = visited or set()
    if value is None or isinstance(value, (str, bytes, int, float, bool)):
        return

    value_id = id(value)
    if value_id in visited:
        return
    visited.add(value_id)

    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_langchain_objects(item, visited)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _iter_langchain_objects(item, visited)
        return
    if not type(value).__module__.startswith("langchain"):
        return

    yield value
    if type(value).__name__ not in _OPENAI_CLIENT_NAMES:
        for field_name, item in getattr(value, "__dict__", {}).items():
            if field_name not in _RUNTIME_FIELDS:
                yield from _iter_langchain_objects(item, visited)


def _secret_value(value) -> Optional[str]:
    if hasattr(value, "get_secret_value"):
        value = value.get_secret_value()
    return value if isinstance(value, str) and value else None


def _collect_secrets(chain, ignored_secret_ids: Set[str]) -> Dict[str, str]:
    secrets = {}
    for value in _iter_langchain_objects(chain):
        for field_name, secret_id in getattr(value, "lc_secrets", {}).items():
            if secret_id in ignored_secret_ids:
                continue
            secret = _secret_value(getattr(value, field_name, None))
            if secret is None:
                continue
            existing = secrets.get(secret_id)
            if existing is not None and existing != secret:
                raise ValueError(
                    "LangChain contains conflicting values for secret "
                    f"{secret_id}. Set one SynapseML subscription key instead."
                )
            secrets[secret_id] = secret
    return secrets


def _openai_sdk_clients(value):
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


def _clear_azure_ad_token_sentinel(chain) -> None:
    for value in _iter_langchain_objects(chain):
        if type(value).__name__ not in _AZURE_OPENAI_CLIENT_NAMES:
            continue
        token = _secret_value(getattr(value, "azure_ad_token", None))
        if token != _AZURE_API_KEY_ONLY_TOKEN:
            continue

        value.azure_ad_token = None
        value.azure_ad_token_provider = None
        value.azure_ad_async_token_provider = None
        for client in _openai_sdk_clients(value):
            client._azure_ad_token = None
            client._azure_ad_token_provider = None


def _service_url(url: str) -> SplitResult:
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("OpenAI URL must be an absolute HTTP or HTTPS URL.")
    if parsed.query or parsed.fragment:
        raise ValueError(
            "OpenAI URL must not contain a query string or fragment. "
            "Set the API version with setApiVersion instead."
        )
    return parsed


def _strip_azure_openai_path(path: str) -> str:
    index = path.find("/openai")
    if index < 0:
        return path
    suffix = index + len("/openai")
    return path[:index] if suffix == len(path) or path[suffix] == "/" else path


def _openai_base_url(url: str) -> str:
    parsed = _service_url(url)
    path = parsed.path.rstrip("/")
    hostname = parsed.hostname or ""
    if hostname.endswith((".openai.azure.com", ".services.ai.azure.com")):
        path = _strip_azure_openai_path(path) + "/openai/v1"
    return urlunsplit(parsed._replace(path=path + "/"))


def _azure_endpoint(url: str) -> str:
    parsed = _service_url(url)
    path = parsed.path.rstrip("/")
    hostname = parsed.hostname or ""
    if hostname.endswith((".openai.azure.com", ".services.ai.azure.com")):
        path = _strip_azure_openai_path(path)
    return urlunsplit(parsed._replace(path=path + "/"))


def _remove_mapping_keys(container, field_name: str, blocked_keys: Set[str]) -> None:
    mapping = container.get(field_name)
    if not isinstance(mapping, dict):
        return
    for key in list(mapping):
        if isinstance(key, str) and key.lower() in blocked_keys:
            mapping.pop(key)
    if not mapping:
        container.pop(field_name)


def _sanitize_request_options(
    kwargs,
    subscription_key: Optional[str],
    url: Optional[str],
    api_version: Optional[str],
    sanitize_transport: bool,
) -> None:
    containers = [kwargs]
    model_kwargs = kwargs.get("model_kwargs")
    if isinstance(model_kwargs, dict):
        containers.append(model_kwargs)

    if sanitize_transport:
        if isinstance(model_kwargs, dict):
            for field_name in _NESTED_UNTRUSTED_TRANSPORT_FIELDS:
                model_kwargs.pop(field_name, None)
        for container in containers:
            container.pop("extra_headers", None)
            container.pop("extra_query", None)
        return

    overridden_fields = set()
    if subscription_key is not None:
        overridden_fields.update(
            (
                "api_key",
                "azure_ad_async_token_provider",
                "azure_ad_token",
                "azure_ad_token_provider",
                "openai_api_key",
            )
        )
    if url is not None:
        overridden_fields.update(("azure_endpoint", "base_url", "openai_api_base"))
    if api_version is not None:
        overridden_fields.update(("api_version", "openai_api_version"))
    for container in containers:
        for field_name in overridden_fields:
            container.pop(field_name, None)

    blocked_headers = set()
    if subscription_key is not None:
        blocked_headers.update(("authorization", "api-key"))
    if url is not None:
        blocked_headers.update(("host", ":authority"))
    blocked_query = {"api-version", "api_version"} if api_version is not None else set()

    for container in containers:
        _remove_mapping_keys(container, "extra_headers", blocked_headers)
        _remove_mapping_keys(container, "extra_query", blocked_query)
        _remove_mapping_keys(container, "default_headers", blocked_headers)
        _remove_mapping_keys(container, "default_query", blocked_query)


def _configure_serialized_chain(
    value,
    subscription_key: Optional[str],
    url: Optional[str],
    api_version: Optional[str],
    secret_ids: Dict[str, str],
    additional_import_mappings: Dict[Tuple[str, ...], Tuple[str, ...]],
    sanitize_transport: bool,
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
            if identifier[0] == "langchain_classic":
                additional_import_mappings[identifier_tuple] = identifier_tuple

            if identifier[-1] in _OPENAI_CLIENT_NAMES:
                kwargs = value.setdefault("kwargs", {})
                is_azure_client = identifier[-1] in _AZURE_OPENAI_CLIENT_NAMES
                if sanitize_transport:
                    for field_name in _UNTRUSTED_TRANSPORT_FIELDS:
                        kwargs.pop(field_name, None)
                _sanitize_request_options(
                    kwargs,
                    subscription_key,
                    url,
                    api_version,
                    sanitize_transport,
                )
                if subscription_key is not None:
                    secret_id = (
                        _AZURE_OPENAI_KEY_SECRET_ID
                        if is_azure_client
                        else _OPENAI_KEY_SECRET_ID
                    )
                    kwargs["openai_api_key"] = {
                        "lc": 1,
                        "type": "secret",
                        "id": [secret_ids[secret_id]],
                    }
                    if is_azure_client:
                        kwargs["azure_ad_token"] = _AZURE_API_KEY_ONLY_TOKEN
                        kwargs.pop("azure_ad_token_provider", None)
                        kwargs.pop("azure_ad_async_token_provider", None)
                if url is not None:
                    if is_azure_client:
                        kwargs["azure_endpoint"] = _azure_endpoint(url)
                        kwargs.pop("openai_api_base", None)
                    else:
                        kwargs["openai_api_base"] = _openai_base_url(url)
                if api_version is not None and is_azure_client:
                    kwargs["openai_api_version"] = api_version

        for item in value.values():
            _configure_serialized_chain(
                item,
                subscription_key,
                url,
                api_version,
                secret_ids,
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
                secret_ids,
                additional_import_mappings,
                sanitize_transport,
            )


def prepare_serialized_chain(
    serialized_chain: str,
    secrets_map: Dict[str, str],
    subscription_key: Optional[str],
    url: Optional[str],
    api_version: Optional[str],
    sanitize_transport: bool = False,
) -> Optional[WorkerChainConfig]:
    serialized_config = json.loads(serialized_chain)

    if sanitize_transport:
        secrets_map = {}
        secret_ids = {
            secret_id: f"SYNAPSEML_{secret_id}_{uuid4().hex}"
            for secret_id in (_OPENAI_KEY_SECRET_ID, _AZURE_OPENAI_KEY_SECRET_ID)
        }
    else:
        secrets_map = secrets_map.copy()
        secret_ids = {
            _OPENAI_KEY_SECRET_ID: _OPENAI_KEY_SECRET_ID,
            _AZURE_OPENAI_KEY_SECRET_ID: _AZURE_OPENAI_KEY_SECRET_ID,
        }
    if subscription_key is not None:
        secrets_map[secret_ids[_OPENAI_KEY_SECRET_ID]] = subscription_key
        secrets_map[secret_ids[_AZURE_OPENAI_KEY_SECRET_ID]] = subscription_key
        secrets_map.pop(_AZURE_OPENAI_AD_TOKEN_SECRET_ID, None)

    additional_import_mappings = {}
    _configure_serialized_chain(
        serialized_config,
        subscription_key,
        url,
        api_version,
        secret_ids,
        additional_import_mappings,
        sanitize_transport,
    )
    if contains_not_implemented(serialized_config):
        return None
    if sanitize_transport:
        _validate_secret_references(serialized_config, set(secrets_map))
    return WorkerChainConfig(
        json.dumps(serialized_config),
        secrets_map,
        additional_import_mappings,
    )


def prepare_chain_for_worker(
    chain,
    subscription_key: Optional[str],
    url: Optional[str],
    api_version: Optional[str],
) -> Optional[WorkerChainConfig]:
    try:
        serialized_chain = dumps(chain)
    except TypeError:
        return None

    ignored_secret_ids = _OPENAI_SECRET_IDS if subscription_key is not None else set()
    return prepare_serialized_chain(
        serialized_chain,
        _collect_secrets(chain, ignored_secret_ids),
        subscription_key,
        url,
        api_version,
    )


def load_chain_for_worker(config: WorkerChainConfig):
    additional_import_mappings = dict(_PERSISTED_IMPORT_MAPPINGS)
    additional_import_mappings.update(config.additional_import_mappings)
    chain = loads(
        config.serialized_chain,
        allowed_objects="all",
        secrets_map=config.secrets_map,
        valid_namespaces=["langchain_classic"],
        additional_import_mappings=additional_import_mappings,
        secrets_from_env=False,
    )
    _clear_azure_ad_token_sentinel(chain)
    return chain


def load_persisted_chain(config: WorkerChainConfig):
    chain = loads(
        config.serialized_chain,
        allowed_objects="core",
        secrets_map=config.secrets_map,
        valid_namespaces=["langchain_classic"],
        additional_import_mappings=_PERSISTED_IMPORT_MAPPINGS,
        secrets_from_env=False,
    )
    _clear_azure_ad_token_sentinel(chain)
    return chain
