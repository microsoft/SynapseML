# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from langchain_classic.chains import LLMChain
from langchain_classic.output_parsers.regex import RegexParser
from langchain_core.load import dumps
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import (
    RunnableLambda,
    RunnableParallel,
    RunnablePassthrough,
)
from langchain_openai import AzureChatOpenAI, AzureOpenAI, ChatOpenAI
from openai import OpenAIError

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.langchain import LangchainTransformer
from synapse.ml.services.langchain._LangchainSerialization import (
    load_chain_for_worker,
    load_persisted_chain,
    prepare_chain_for_worker,
    prepare_serialized_chain,
)
from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults

spark = init_spark()


@contextmanager
def openai_server():
    requests = []

    class OpenAIHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            content_length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(content_length)
            requests.append(
                {
                    "authorization": self.headers.get("Authorization"),
                    "api_key": self.headers.get("api-key"),
                    "path": self.path,
                }
            )
            if "/chat/completions" in self.path:
                choices = [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "configured",
                        },
                        "finish_reason": "stop",
                    }
                ]
                object_type = "chat.completion"
                response_id = "chatcmpl-test"
            else:
                choices = [
                    {
                        "index": 0,
                        "text": "configured",
                        "finish_reason": "stop",
                        "logprobs": None,
                    }
                ]
                object_type = "text_completion"
                response_id = "cmpl-test"
            response = json.dumps(
                {
                    "id": response_id,
                    "object": object_type,
                    "created": 1,
                    "model": "gpt-5.1",
                    "choices": choices,
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2,
                    },
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, _, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), OpenAIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port, requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class LangchainRunnableTest(unittest.TestCase):
    def tearDown(self):
        defaults = OpenAIDefaults()
        defaults.reset_subscription_key()
        defaults.reset_URL()
        defaults.reset_api_version()

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

    def test_transformer_applies_inline_settings_on_workers(self):
        with openai_server() as (port, requests):
            llm = ChatOpenAI(
                model="gpt-5.1",
                base_url="http://127.0.0.1:1/v1/",
                api_key="old-key",
                max_retries=0,
            )
            try:
                transformer = (
                    LangchainTransformer()
                    .setInputCol("value")
                    .setOutputCol("result")
                    .setChain(llm)
                    .setSubscriptionKey("inline-key")
                    .setUrl(f"http://127.0.0.1:{port}/v1/")
                )

                rows = transformer.transform(
                    spark.createDataFrame(
                        [("first",), ("second",)], ["value"]
                    ).repartition(2)
                ).collect()
            finally:
                llm.client._client.close()

        self.assertEqual([row.result for row in rows], ["configured", "configured"])
        self.assertEqual(len(requests), 2)
        self.assertTrue(
            all(request["authorization"] == "Bearer inline-key" for request in requests)
        )
        self.assertTrue(
            all(request["path"] == "/v1/chat/completions" for request in requests)
        )

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

    def test_transformer_azure_key_override_disables_ad_token_auth(self):
        with openai_server() as (port, requests):
            llm = AzureOpenAI(
                model="gpt-5.1",
                azure_endpoint="http://127.0.0.1:1/",
                azure_deployment="deployment",
                api_version="old-version",
                api_key="old-key",
                max_retries=0,
            )
            try:
                transformer = (
                    LangchainTransformer()
                    .setInputCol("value")
                    .setOutputCol("result")
                    .setChain(llm)
                    .setSubscriptionKey("inline-key")
                    .setUrl(f"http://127.0.0.1:{port}/")
                    .setApiVersion("new-version")
                )

                row = transformer.transform(
                    spark.createDataFrame([("test",)], ["value"])
                ).first()
            finally:
                llm.client._client.close()

        self.assertEqual(row.result, "configured")
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["api_key"], "inline-key")
        self.assertTrue(
            requests[0]["path"].startswith("/openai/deployments/deployment/completions")
        )

    def test_inline_settings_override_openai_defaults(self):
        defaults = OpenAIDefaults()
        defaults.set_subscription_key("global-key")
        defaults.set_URL("https://global.openai.azure.com/")
        defaults.set_api_version("global-version")

        transformer = LangchainTransformer()
        self.assertEqual(
            transformer._get_effective_openai_settings()[:3],
            (
                "global-key",
                "https://global.openai.azure.com/",
                "global-version",
            ),
        )

        transformer.setSubscriptionKey("inline-key")
        transformer.setUrl("https://inline.openai.azure.com/")
        transformer.setApiVersion("inline-version")
        self.assertEqual(
            transformer._get_effective_openai_settings()[:3],
            (
                "inline-key",
                "https://inline.openai.azure.com/",
                "inline-version",
            ),
        )

    def test_worker_reconstruction_preserves_direct_client(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://proxy.example.com/custom/openai/",
            api_key="direct-key",
        )
        loaded = None
        try:
            config = prepare_chain_for_worker(llm, None, None, None)
            self.assertIsNotNone(config)
            loaded = load_chain_for_worker(config)
            self.assertEqual(
                str(loaded.root_client.base_url),
                "https://proxy.example.com/custom/openai/",
            )
            self.assertEqual(loaded.openai_api_key.get_secret_value(), "direct-key")
        finally:
            llm.root_client.close()
            if loaded is not None:
                loaded.root_client.close()

    def test_worker_reconstruction_normalizes_azure_urls(self):
        chat_llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://old.openai.azure.com/openai/v1/",
            api_key="old-key",
        )
        azure_llm = AzureChatOpenAI(
            model="gpt-5.1",
            azure_endpoint="https://old.openai.azure.com/",
            api_version="old-version",
            api_key="old-key",
            azure_ad_token_provider=lambda: "old-token",
            azure_ad_async_token_provider=lambda: "old-async-token",
        )
        loaded_chat = None
        loaded_azure = None
        try:
            loaded_chat = load_chain_for_worker(
                prepare_chain_for_worker(
                    chat_llm,
                    "new-key",
                    "https://new.openai.azure.com/openai/",
                    None,
                )
            )
            azure_config = prepare_chain_for_worker(
                azure_llm,
                "new-key",
                "https://new.openai.azure.com/openai/",
                "new-version",
            )
            self.assertIsNotNone(azure_config)
            loaded_azure = load_chain_for_worker(azure_config)

            self.assertEqual(
                str(loaded_chat.root_client.base_url),
                "https://new.openai.azure.com/openai/v1/",
            )
            self.assertEqual(
                loaded_azure.azure_endpoint,
                "https://new.openai.azure.com/",
            )
            self.assertEqual(loaded_azure.openai_api_version, "new-version")
            self.assertIsNone(loaded_azure.root_client._azure_ad_token)
            self.assertIsNone(loaded_azure.root_async_client._azure_ad_token)
        finally:
            chat_llm.root_client.close()
            azure_llm.root_client.close()
            if loaded_chat is not None:
                loaded_chat.root_client.close()
            if loaded_azure is not None:
                loaded_azure.root_client.close()

    def test_worker_reconstruction_rejects_conflicting_direct_keys(self):
        first = ChatOpenAI(model="gpt-5.1", api_key="first-key")
        second = ChatOpenAI(model="gpt-5.1", api_key="second-key")
        try:
            chain = RunnableParallel(first=first, second=second)
            with self.assertRaisesRegex(ValueError, "conflicting values"):
                prepare_chain_for_worker(chain, None, None, None)
        finally:
            first.root_client.close()
            second.root_client.close()

    def test_worker_reconstruction_clears_azure_completion_token(self):
        llm = AzureOpenAI(
            model="gpt-5.1",
            azure_endpoint="https://old.openai.azure.com/",
            azure_deployment="deployment",
            api_version="old-version",
            api_key="old-key",
        )
        loaded = None
        try:
            loaded = load_chain_for_worker(
                prepare_chain_for_worker(
                    llm,
                    "trusted-key",
                    "https://trusted.openai.azure.com/",
                    "trusted-version",
                )
            )

            self.assertIsNone(loaded.client._client._azure_ad_token)
            self.assertIsNone(loaded.async_client._client._azure_ad_token)
        finally:
            llm.client._client.close()
            if loaded is not None:
                loaded.client._client.close()

    def test_worker_reconstruction_supports_classic_components(self):
        parser = RegexParser(regex=r"(.*)", output_keys=["value"])
        config = prepare_chain_for_worker(parser, None, None, None)

        loaded = load_chain_for_worker(config)

        self.assertIsInstance(loaded, RegexParser)
        self.assertEqual(loaded.invoke("classic"), {"value": "classic"})

    def test_worker_settings_override_nested_request_transport(self):
        llm = AzureChatOpenAI(
            model="gpt-5.1",
            azure_endpoint="https://old.openai.azure.com/",
            api_version="old-version",
            api_key="old-key",
            model_kwargs={
                "extra_headers": {
                    "api-key": "old-key",
                    "Host": "old.openai.azure.com",
                    "X-Keep": "header",
                },
                "extra_query": {
                    "api-version": "old-version",
                    "keep": "query",
                },
            },
        )
        loaded = None
        try:
            loaded = load_chain_for_worker(
                prepare_chain_for_worker(
                    llm,
                    "trusted-key",
                    "https://trusted.openai.azure.com/",
                    "trusted-version",
                )
            )

            self.assertEqual(
                loaded.model_kwargs["extra_headers"],
                {"X-Keep": "header"},
            )
            self.assertEqual(
                loaded.model_kwargs["extra_query"],
                {"keep": "query"},
            )
        finally:
            llm.root_client.close()
            if loaded is not None:
                loaded.root_client.close()

    def test_transformer_saves_serializable_runnable(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(PromptTemplate.from_template("Define {value}"))
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / "langchain-transformer")
            transformer.save(path)
            loaded_transformer = LangchainTransformer.load(path)

        self.assertIsInstance(loaded_transformer.getChain(), PromptTemplate)
        self.assertEqual(loaded_transformer.getInputCol(), "value")
        self.assertEqual(loaded_transformer.getOutputCol(), "result")

    def test_writer_sanitizes_transport_before_persisting(self):
        llm = AzureChatOpenAI(
            model="gpt-5.1",
            azure_endpoint="https://artifact.openai.azure.com/",
            api_version="artifact-version",
            api_key="artifact-key",
            azure_ad_token_provider=lambda: "artifact-token",
            azure_ad_async_token_provider=lambda: "artifact-async-token",
            default_headers={"Authorization": "artifact-key"},
            model_kwargs={
                "extra_headers": {"api-key": "artifact-key"},
                "extra_query": {"api-version": "artifact-version"},
            },
        )
        try:
            serialized = LangchainTransformer().write()._chain_serializer(llm)
            serialized_config = json.loads(serialized)

            self.assertNotIn("artifact-key", serialized)
            self.assertNotIn("artifact-token", serialized)
            self.assertNotIn("artifact-async-token", serialized)
            self.assertNotIn("not_implemented", serialized)
            self.assertNotIn("azure_endpoint", serialized_config["kwargs"])
            self.assertNotIn("default_headers", serialized_config["kwargs"])
            self.assertNotIn(
                "extra_headers", serialized_config["kwargs"]["model_kwargs"]
            )
            self.assertNotIn("extra_query", serialized_config["kwargs"]["model_kwargs"])
        finally:
            llm.root_client.close()

    def test_transformer_saves_legacy_openai_chain_with_trusted_settings(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://old.openai.azure.com/openai/v1/",
            api_key="old-key",
            default_headers={"X-Untrusted": "header"},
            default_query={"untrusted": "query"},
            model_kwargs={
                "extra_headers": {
                    "Host": "attacker.example",
                    "api-key": "artifact-key",
                },
                "extra_query": {
                    "api-version": "artifact-version",
                    "artifact": "query",
                },
            },
        )
        loaded_transformer = None
        try:
            transformer = (
                LangchainTransformer()
                .setInputCol("value")
                .setOutputCol("result")
                .setChain(
                    LLMChain(
                        llm=llm,
                        prompt=PromptTemplate.from_template("{value}"),
                    )
                )
                .setSubscriptionKey("inline-key")
                .setUrl("https://new.openai.azure.com/")
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                path = str(Path(temp_dir) / "langchain-transformer")
                transformer.save(path)
                loaded_transformer = LangchainTransformer.load(path)

            loaded_llm = loaded_transformer.getChain().llm
            self.assertEqual(
                str(loaded_llm.root_client.base_url),
                "https://new.openai.azure.com/openai/v1/",
            )
            self.assertEqual(loaded_llm.openai_api_key.get_secret_value(), "inline-key")
            self.assertIsNone(loaded_llm.default_headers)
            self.assertIsNone(loaded_llm.default_query)
            self.assertNotIn("extra_headers", loaded_llm.model_kwargs)
            self.assertNotIn("extra_query", loaded_llm.model_kwargs)
        finally:
            llm.root_client.close()
            if loaded_transformer is not None:
                loaded_transformer.getChain().llm.root_client.close()

    def test_persisted_chain_strips_nested_transport_aliases(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://old.openai.azure.com/openai/v1/",
            api_key="old-key",
        )
        loaded = None
        try:
            manifest = json.loads(dumps(llm))
            manifest["kwargs"]["model_kwargs"] = {
                "api_key": "artifact-key",
                "base_url": "https://attacker.example/v1/",
                "default_headers": {"Authorization": "artifact-key"},
                "default_query": {"api-version": "artifact-version"},
                "openai_api_version": "artifact-version",
            }
            config = prepare_serialized_chain(
                json.dumps(manifest),
                {},
                "trusted-key",
                "https://trusted.openai.azure.com/",
                None,
                sanitize_transport=True,
            )

            loaded = load_persisted_chain(config)

            self.assertEqual(
                str(loaded.root_client.base_url),
                "https://trusted.openai.azure.com/openai/v1/",
            )
            self.assertEqual(loaded.openai_api_key.get_secret_value(), "trusted-key")
            self.assertFalse(
                set(manifest["kwargs"]["model_kwargs"]) & set(loaded.model_kwargs)
            )
        finally:
            llm.root_client.close()
            if loaded is not None:
                loaded.root_client.close()

    def test_saved_azure_chain_preserves_chain_api_version(self):
        llm = AzureChatOpenAI(
            model="gpt-5.1",
            azure_endpoint="https://old.openai.azure.com/",
            api_version="chain-version",
            api_key="old-key",
        )
        loaded_transformer = None
        try:
            transformer = (
                LangchainTransformer()
                .setInputCol("value")
                .setOutputCol("result")
                .setChain(llm)
                .setSubscriptionKey("trusted-key")
                .setUrl("https://trusted.openai.azure.com/")
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                path = str(Path(temp_dir) / "langchain-transformer")
                transformer.save(path)
                loaded_transformer = LangchainTransformer.load(path)

            self.assertEqual(
                loaded_transformer.getChain().openai_api_version,
                "chain-version",
            )
        finally:
            llm.root_client.close()
            if loaded_transformer is not None:
                loaded_transformer.getChain().root_client.close()

    def test_saved_chain_cannot_reference_trusted_openai_secret(self):
        prompt = PromptTemplate.from_template("{value}:{leak}")
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://old.openai.azure.com/openai/v1/",
            api_key="old-key",
        )
        try:
            manifest = json.loads(dumps(prompt))
            manifest["kwargs"]["partial_variables"] = {
                "leak": {
                    "lc": 1,
                    "type": "secret",
                    "id": ["OPENAI_API_KEY"],
                }
            }
            manifest["kwargs"]["metadata"] = {
                "client": json.loads(dumps(llm)),
            }

            with self.assertRaisesRegex(
                ValueError, "cannot reference external secrets"
            ):
                prepare_serialized_chain(
                    json.dumps(manifest),
                    {},
                    "trusted-secret",
                    "https://trusted.openai.azure.com/",
                    None,
                    sanitize_transport=True,
                )
        finally:
            llm.root_client.close()

    def test_saved_chain_uses_internal_default_url(self):
        class InternalLangchainTransformer(LangchainTransformer):
            def __init__(self):
                super().__init__()
                self.running_on_synapse_internal = True
                self._setDefault(url="https://internal.openai.azure.com/")

        defaults = OpenAIDefaults()
        cases = (
            (
                "global-url",
                None,
                "global-key",
                "https://global.openai.azure.com/",
                "https://global.openai.azure.com/openai/v1/",
            ),
            (
                "global-internal",
                None,
                "global-key",
                None,
                "https://internal.openai.azure.com/openai/v1/",
            ),
            (
                "saved-internal",
                "saved-key",
                None,
                None,
                "https://internal.openai.azure.com/openai/v1/",
            ),
        )
        for (
            name,
            saved_key,
            global_key,
            global_url,
            expected_url,
        ) in cases:
            with self.subTest(name=name):
                defaults.reset_subscription_key()
                defaults.reset_URL()
                if global_key is not None:
                    defaults.set_subscription_key(global_key)
                if global_url is not None:
                    defaults.set_URL(global_url)
                llm = ChatOpenAI(
                    model="gpt-5.1",
                    base_url="https://artifact.example/v1/",
                    api_key="artifact-key",
                )
                loaded_transformer = None
                try:
                    transformer = (
                        LangchainTransformer()
                        .setInputCol("value")
                        .setOutputCol("result")
                        .setChain(llm)
                    )
                    if saved_key is not None:
                        transformer.setSubscriptionKey(saved_key)
                    else:
                        transformer.setUrl("https://attacker.example/")
                    with tempfile.TemporaryDirectory() as temp_dir:
                        path = str(Path(temp_dir) / "langchain-transformer")
                        transformer.save(path)
                        with patch(
                            "synapse.ml.services.langchain.LangchainTransform.secure_import_class",
                            return_value=InternalLangchainTransformer,
                        ):
                            loaded_transformer = LangchainTransformer.load(path)

                    loaded_llm = loaded_transformer.getChain()
                    self.assertEqual(
                        str(loaded_llm.root_client.base_url),
                        expected_url,
                    )
                    self.assertEqual(
                        loaded_llm.openai_api_key.get_secret_value(),
                        saved_key or global_key,
                    )
                finally:
                    llm.root_client.close()
                    if loaded_transformer is not None:
                        loaded_transformer.getChain().root_client.close()

    def test_saved_openai_chain_requires_key_and_url(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://saved.openai.azure.com/openai/v1/",
            api_key="saved-key",
        )
        try:
            transformer = (
                LangchainTransformer()
                .setInputCol("value")
                .setOutputCol("result")
                .setChain(llm)
            )
            with tempfile.TemporaryDirectory() as temp_dir:
                path = str(Path(temp_dir) / "langchain-transformer")
                transformer.save(path)
                OpenAIDefaults().set_subscription_key("global-key")
                with self.assertRaisesRegex(ValueError, "requires both"):
                    LangchainTransformer.load(path)
        finally:
            llm.root_client.close()

    def test_transformer_rejects_non_serializable_runnable_on_save(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(RunnableLambda(lambda value: value))
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / "langchain-transformer")
            with self.assertRaisesRegex(
                NotImplementedError, "cannot be serialized by langchain-core"
            ):
                transformer.save(path)

    def test_transformer_rejects_unsupported_classic_component_on_save(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(RegexParser(regex=r"(.*)", output_keys=["value"]))
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / "langchain-transformer")
            with self.assertRaisesRegex(
                NotImplementedError, "cannot be serialized by langchain-core"
            ):
                transformer.save(path)

    def test_transformer_rejects_plain_callable(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(lambda value: value)
        )

        with self.assertRaisesRegex(TypeError, "must define invoke"):
            transformer.transform(spark.createDataFrame([("test",)], ["value"]))


if __name__ == "__main__":
    unittest.main()
