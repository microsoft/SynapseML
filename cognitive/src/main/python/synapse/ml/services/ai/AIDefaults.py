# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from typing import Dict, Optional, TypeVar

from pyspark import SparkContext
from pyspark.ml.wrapper import JavaParams, _py2java

_Stage = TypeVar("_Stage", bound=JavaParams)


class AIDefaults:
    """Driver-JVM defaults, shared with Scala and every Spark session in that JVM.

    The ``openai`` scope uses the same store as ``OpenAIDefaults``. Other scopes
    isolate API-specific settings. Configure before constructing Spark actions.
    """

    def __init__(self):
        self._sc = SparkContext.getOrCreate()
        self._defaults = (
            self._sc._jvm.com.microsoft.azure.synapse.ml.services.ai.AIDefaults
        )

    def for_api(self, api: str) -> "AIDefaultsScope":
        """Select openai, chat_completions, responses, embeddings, systemone,
        decisions, or content_understanding. Unknown names are rejected.
        """
        return AIDefaultsScope(self._sc, self._defaults.forApi(api))

    def reset_all(self) -> None:
        """Reset all AI scopes, including the shared legacy OpenAI defaults."""
        self._defaults.resetAll()


class AIDefaultsScope:
    """Typed scalar settings; unsupported fields are rejected by the JVM."""

    def __init__(self, sc, defaults):
        self._sc = sc
        self._defaults = defaults

    def _set(self, method, value):
        getattr(self._defaults, method)(value)
        return self

    def _get(self, method):
        option = getattr(self._defaults, method)()
        return option.get() if option.isDefined() else None

    def reset(self) -> None:
        self._defaults.reset()

    def set_endpoint(self, value: str) -> "AIDefaultsScope":
        return self._set("setEndpoint", value)

    def get_endpoint(self) -> Optional[str]:
        return self._get("getEndpoint")

    def reset_endpoint(self) -> None:
        self._defaults.resetEndpoint()

    def set_api_key(self, value: str) -> "AIDefaultsScope":
        return self._set("setApiKey", value)

    def get_api_key(self) -> Optional[str]:
        return self._get("getApiKey")

    def reset_api_key(self) -> None:
        self._defaults.resetApiKey()

    def set_model(self, value: str) -> "AIDefaultsScope":
        return self._set("setModel", value)

    def get_model(self) -> Optional[str]:
        return self._get("getModel")

    def reset_model(self) -> None:
        self._defaults.resetModel()

    def set_deployment_name(self, value: str) -> "AIDefaultsScope":
        return self._set("setDeploymentName", value)

    def get_deployment_name(self) -> Optional[str]:
        return self._get("getDeploymentName")

    def reset_deployment_name(self) -> None:
        self._defaults.resetDeploymentName()

    def set_api_version(self, value: str) -> "AIDefaultsScope":
        return self._set("setApiVersion", value)

    def get_api_version(self) -> Optional[str]:
        return self._get("getApiVersion")

    def reset_api_version(self) -> None:
        self._defaults.resetApiVersion()

    def set_api_type(self, value: str) -> "AIDefaultsScope":
        return self._set("setApiType", value)

    def get_api_type(self) -> Optional[str]:
        return self._get("getApiType")

    def reset_api_type(self) -> None:
        self._defaults.resetApiType()

    def set_embedding_deployment_name(self, value: str) -> "AIDefaultsScope":
        return self._set("setEmbeddingDeploymentName", value)

    def get_embedding_deployment_name(self) -> Optional[str]:
        return self._get("getEmbeddingDeploymentName")

    def reset_embedding_deployment_name(self) -> None:
        self._defaults.resetEmbeddingDeploymentName()

    def set_temperature(self, value: float) -> "AIDefaultsScope":
        return self._set("setTemperature", float(value))

    def get_temperature(self) -> Optional[float]:
        return self._get("getTemperature")

    def reset_temperature(self) -> None:
        self._defaults.resetTemperature()

    def set_top_p(self, value: float) -> "AIDefaultsScope":
        return self._set("setTopP", float(value))

    def get_top_p(self) -> Optional[float]:
        return self._get("getTopP")

    def reset_top_p(self) -> None:
        self._defaults.resetTopP()

    def set_seed(self, value: int) -> "AIDefaultsScope":
        if type(value) is not int:
            raise TypeError("seed must be an integer")
        return self._set("setSeed", value)

    def get_seed(self) -> Optional[int]:
        return self._get("getSeed")

    def reset_seed(self) -> None:
        self._defaults.resetSeed()

    def set_verbosity(self, value: str) -> "AIDefaultsScope":
        return self._set("setVerbosity", value)

    def get_verbosity(self) -> Optional[str]:
        return self._get("getVerbosity")

    def reset_verbosity(self) -> None:
        self._defaults.resetVerbosity()

    def set_reasoning_effort(self, value: str) -> "AIDefaultsScope":
        return self._set("setReasoningEffort", value)

    def get_reasoning_effort(self) -> Optional[str]:
        return self._get("getReasoningEffort")

    def reset_reasoning_effort(self) -> None:
        self._defaults.resetReasoningEffort()

    def set_analyzer_id(self, value: str) -> "AIDefaultsScope":
        return self._set("setAnalyzerId", value)

    def get_analyzer_id(self) -> Optional[str]:
        return self._get("getAnalyzerId")

    def reset_analyzer_id(self) -> None:
        self._defaults.resetAnalyzerId()

    def set_model_deployments(self, value: Dict[str, str]) -> "AIDefaultsScope":
        if not isinstance(value, dict) or any(
            not isinstance(k, str) or not isinstance(v, str) for k, v in value.items()
        ):
            raise TypeError("model_deployments must be a dictionary of strings")
        return self._set("setModelDeployments", _py2java(self._sc, value))

    def get_model_deployments(self) -> Optional[Dict[str, str]]:
        value = self._get("getModelDeployments")
        if value is None:
            return None
        result = {}
        entries = value.iterator()
        while entries.hasNext():
            entry = entries.next()
            result[entry._1()] = entry._2()
        return result

    def reset_model_deployments(self) -> None:
        self._defaults.resetModelDeployments()

    def set_string_encoding(self, value: str) -> "AIDefaultsScope":
        return self._set("setStringEncoding", value)

    def get_string_encoding(self) -> Optional[str]:
        return self._get("getStringEncoding")

    def reset_string_encoding(self) -> None:
        self._defaults.resetStringEncoding()

    def set_processing_location(self, value: str) -> "AIDefaultsScope":
        return self._set("setProcessingLocation", value)

    def get_processing_location(self) -> Optional[str]:
        return self._get("getProcessingLocation")

    def reset_processing_location(self) -> None:
        self._defaults.resetProcessingLocation()

    def set_allow_input_truncation(self, value: bool) -> "AIDefaultsScope":
        if type(value) is not bool:
            raise TypeError("allow_input_truncation must be a boolean")
        return self._set("setAllowInputTruncation", value)

    def get_allow_input_truncation(self) -> Optional[bool]:
        return self._get("getAllowInputTruncation")

    def reset_allow_input_truncation(self) -> None:
        self._defaults.resetAllowInputTruncation()

    def apply_to(self, stage: _Stage) -> _Stage:
        """Copy ContentUnderstanding with explicit params winning over defaults.

        The copy snapshots configuration, including credentials. Saving that
        copy persists those values just like calling the stage's own setters.
        """
        result = stage.copy({})
        result._java_obj = self._defaults.applyTo(result._java_obj)
        result._transfer_params_from_java()
        return result
