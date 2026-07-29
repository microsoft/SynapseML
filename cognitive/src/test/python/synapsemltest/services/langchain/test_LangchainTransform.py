# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from langchain_classic.chains import LLMChain
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import (
    RunnableLambda,
    RunnableParallel,
    RunnablePassthrough,
)
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from openai import OpenAIError

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.langchain import LangchainTransformer
from synapse.ml.services.langchain.LangchainTransform import (
    _load_chain_for_worker,
    _load_persisted_chain,
    _prepare_chain_for_worker,
    _prepare_serialized_chain_for_worker,
    LangchainTransformerParamsReader,
)
from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults

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

    def test_transformer_reconstructs_serializable_runnable(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(RunnablePassthrough())
        )

        row = transformer.transform(
            spark.createDataFrame([("test",)], ["value"])
        ).first()

        self.assertEqual(row.result, "test")
        self.assertEqual(row.errorCol, "")

    def test_transformer_applies_inline_openai_settings_on_workers(self):
        requests = []

        class OpenAIHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                content_length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(content_length)
                requests.append(
                    {
                        "authorization": self.headers.get("Authorization"),
                        "path": self.path,
                    }
                )
                response = json.dumps(
                    {
                        "id": "chatcmpl-test",
                        "object": "chat.completion",
                        "created": 1,
                        "model": "gpt-5.1",
                        "choices": [
                            {
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "content": "configured",
                                },
                                "finish_reason": "stop",
                            }
                        ],
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
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

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
                .setUrl(f"http://127.0.0.1:{server.server_port}/v1/")
            )

            rows = transformer.transform(
                spark.createDataFrame([("first",), ("second",)], ["value"]).repartition(
                    2
                )
            ).collect()

            self.assertEqual([row.result for row in rows], ["configured", "configured"])
            self.assertEqual(len(requests), 2)
            self.assertTrue(
                all(
                    request["authorization"] == "Bearer inline-key"
                    for request in requests
                )
            )
            self.assertTrue(
                all(request["path"] == "/v1/chat/completions" for request in requests)
            )
        finally:
            llm.root_client.close()
            server.shutdown()
            server.server_close()
            server_thread.join()

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

    def test_transformer_saves_and_loads_openai_client(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://old.openai.azure.com/openai/v1/",
            api_key="old-key",
            default_headers={"X-Untrusted": "header"},
            default_query={"untrusted": "query"},
            max_retries=0,
        )
        loaded_transformer = None
        try:
            transformer = (
                LangchainTransformer()
                .setInputCol("value")
                .setOutputCol("result")
                .setChain(llm)
                .setSubscriptionKey("inline-key")
                .setUrl("https://new.openai.azure.com/")
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                path = str(Path(temp_dir) / "langchain-transformer")
                transformer.save(path)
                loaded_transformer = LangchainTransformer.load(path)

            loaded_llm = loaded_transformer.getChain()
            self.assertIsInstance(loaded_llm, ChatOpenAI)
            self.assertEqual(
                str(loaded_llm.root_client.base_url),
                "https://new.openai.azure.com/openai/v1/",
            )
            self.assertEqual(loaded_llm.openai_api_key.get_secret_value(), "inline-key")
            self.assertIsNone(loaded_llm.default_headers)
            self.assertIsNone(loaded_llm.default_query)
        finally:
            llm.root_client.close()
            if loaded_transformer is not None:
                loaded_transformer.getChain().root_client.close()

    def test_saved_openai_client_uses_global_key_only_with_global_url(self):
        defaults = OpenAIDefaults()
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://saved.openai.azure.com/openai/v1/",
            api_key="saved-key",
            max_retries=0,
        )
        loaded_transformer = None
        try:
            transformer = (
                LangchainTransformer()
                .setInputCol("value")
                .setOutputCol("result")
                .setChain(llm)
                .setUrl("https://untrusted.openai.azure.com/")
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                path = str(Path(temp_dir) / "langchain-transformer")
                transformer.save(path)

                defaults.set_subscription_key("global-key")
                with self.assertRaisesRegex(
                    ValueError, "also requires OpenAIDefaults.set_URL"
                ):
                    LangchainTransformer.load(path)

                defaults.set_URL("https://trusted.openai.azure.com/")
                reader = LangchainTransformer.read()
                metadata = reader.loadMetadata(path, reader.sc)
                metadata["defaultParamMap"]["url"] = "https://default-attacker.example/"
                with patch.object(
                    LangchainTransformerParamsReader,
                    "loadMetadata",
                    return_value=metadata,
                ):
                    loaded_transformer = reader.load(path)

            loaded_llm = loaded_transformer.getChain()
            self.assertEqual(
                str(loaded_llm.root_client.base_url),
                "https://trusted.openai.azure.com/openai/v1/",
            )
            self.assertEqual(loaded_llm.openai_api_key.get_secret_value(), "global-key")
            self.assertEqual(
                loaded_transformer._get_effective_openai_settings(),
                (
                    "global-key",
                    "https://trusted.openai.azure.com/",
                    None,
                    True,
                ),
            )
            defaults.reset_URL()
            self.assertEqual(
                loaded_transformer._get_effective_openai_settings(),
                ("global-key", None, None, False),
            )
        finally:
            defaults.reset_subscription_key()
            defaults.reset_URL()
            llm.root_client.close()
            if loaded_transformer is not None:
                loaded_transformer.getChain().root_client.close()

    def test_persisted_openai_aliases_cannot_override_trusted_config(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://saved.openai.azure.com/openai/v1/",
            api_key="saved-key",
            max_retries=0,
        )
        restored_llm = None
        try:
            prepared_chain = _prepare_chain_for_worker(
                llm,
                subscription_key=None,
                url=None,
                api_version=None,
            )
            serialized_config = json.loads(prepared_chain.serialized_chain)
            serialized_config["kwargs"].update(
                {
                    "api_key": "artifact-key",
                    "base_url": "https://attacker.example/v1/",
                    "default_headers": {"X-Artifact": "header"},
                    "default_query": {"artifact": "query"},
                    "organization": "artifact-org",
                }
            )
            sanitized_chain = _prepare_serialized_chain_for_worker(
                json.dumps(serialized_config),
                {},
                subscription_key="trusted-key",
                url="https://trusted.openai.azure.com/",
                api_version=None,
                sanitize_transport=True,
            )

            restored_llm = _load_persisted_chain(sanitized_chain)

            self.assertEqual(
                str(restored_llm.root_client.base_url),
                "https://trusted.openai.azure.com/openai/v1/",
            )
            self.assertEqual(
                restored_llm.openai_api_key.get_secret_value(), "trusted-key"
            )
            self.assertIsNone(restored_llm.default_headers)
            self.assertIsNone(restored_llm.default_query)
            self.assertIsNone(restored_llm.openai_organization)
        finally:
            llm.root_client.close()
            if restored_llm is not None:
                restored_llm.root_client.close()

    def test_transformer_saves_and_loads_legacy_llm_chain(self):
        llm = AzureChatOpenAI(
            model="gpt-5.1",
            azure_deployment="deployment",
            azure_endpoint="https://old.openai.azure.com/",
            api_key="old-key",
            api_version="old-version",
            max_retries=0,
        )
        chain = LLMChain(
            llm=llm,
            prompt=PromptTemplate.from_template("{technology}"),
        )
        loaded_transformer = None
        try:
            transformer = (
                LangchainTransformer()
                .setInputCol("technology")
                .setOutputCol("result")
                .setChain(chain)
                .setSubscriptionKey("inline-key")
                .setUrl("https://new.openai.azure.com/")
                .setApiVersion("new-version")
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                path = str(Path(temp_dir) / "langchain-transformer")
                transformer.save(path)
                loaded_transformer = LangchainTransformer.load(path)

            loaded_chain = loaded_transformer.getChain()
            self.assertIsInstance(loaded_chain, LLMChain)
            self.assertEqual(
                str(loaded_chain.llm.root_client.base_url),
                "https://new.openai.azure.com/openai/deployments/deployment/",
            )
            self.assertEqual(
                loaded_chain.llm.openai_api_key.get_secret_value(), "inline-key"
            )
            self.assertEqual(loaded_chain.llm.openai_api_version, "new-version")
        finally:
            llm.root_client.close()
            if loaded_transformer is not None:
                loaded_transformer.getChain().llm.root_client.close()

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

    def test_transformer_rejects_plain_callable(self):
        transformer = (
            LangchainTransformer()
            .setInputCol("value")
            .setOutputCol("result")
            .setChain(lambda value: value)
        )

        with self.assertRaisesRegex(TypeError, "must define invoke"):
            transformer.transform(spark.createDataFrame([("test",)], ["value"]))

    def test_worker_reconstruction_preserves_direct_openai_configuration(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://example.openai.azure.com/openai/v1/",
            api_key="test-key",
            max_retries=0,
        )
        chain = PromptTemplate.from_template("{value}") | llm | StrOutputParser()
        restored_chain = None
        try:
            prepared_chain = _prepare_chain_for_worker(
                chain,
                subscription_key=None,
                url=None,
                api_version=None,
            )
            self.assertIsNotNone(prepared_chain)
            self.assertNotIn("test-key", prepared_chain.serialized_chain)

            restored_chain = _load_chain_for_worker(prepared_chain)
            restored_llm = restored_chain.steps[1]

            self.assertEqual(
                str(restored_llm.root_client.base_url),
                "https://example.openai.azure.com/openai/v1/",
            )
            self.assertEqual(restored_llm.openai_api_key.get_secret_value(), "test-key")
        finally:
            llm.root_client.close()
            if restored_chain is not None:
                restored_chain.steps[1].root_client.close()

    def test_worker_reconstruction_applies_inline_azure_configuration(self):
        llm = AzureChatOpenAI(
            model="gpt-5.1",
            azure_deployment="old-deployment",
            azure_endpoint="https://old.openai.azure.com/",
            api_key="old-key",
            api_version="old-version",
            max_retries=0,
        )
        chain = LLMChain(
            llm=llm,
            prompt=PromptTemplate.from_template("{technology}"),
        )
        restored_chain = None
        try:
            prepared_chain = _prepare_chain_for_worker(
                chain,
                subscription_key="inline-key",
                url="https://new.openai.azure.com/openai/v1/",
                api_version="new-version",
            )
            self.assertIsNotNone(prepared_chain)
            self.assertNotIn("inline-key", prepared_chain.serialized_chain)
            serialized_config = json.loads(prepared_chain.serialized_chain)
            self.assertNotIn("old-key", json.dumps(serialized_config))

            restored_chain = _load_chain_for_worker(prepared_chain)
            restored_llm = restored_chain.llm

            self.assertEqual(
                str(restored_llm.root_client.base_url),
                "https://new.openai.azure.com/openai/deployments/old-deployment/",
            )
            self.assertEqual(
                restored_llm.openai_api_key.get_secret_value(), "inline-key"
            )
            self.assertEqual(restored_llm.openai_api_version, "new-version")
        finally:
            llm.root_client.close()
            if restored_chain is not None:
                restored_chain.llm.root_client.close()

    def test_worker_reconstruction_normalizes_inline_azure_v1_url(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://old.openai.azure.com/openai/v1/",
            api_key="old-key",
            max_retries=0,
        )
        restored_llm = None
        try:
            prepared_chain = _prepare_chain_for_worker(
                llm,
                subscription_key="inline-key",
                url="https://new.openai.azure.com/openai/",
                api_version=None,
            )
            self.assertIsNotNone(prepared_chain)

            restored_llm = _load_chain_for_worker(prepared_chain)

            self.assertEqual(
                str(restored_llm.root_client.base_url),
                "https://new.openai.azure.com/openai/v1/",
            )
            self.assertEqual(
                restored_llm.openai_api_key.get_secret_value(), "inline-key"
            )
        finally:
            llm.root_client.close()
            if restored_llm is not None:
                restored_llm.root_client.close()

    def test_worker_reconstruction_rejects_conflicting_direct_keys(self):
        first_llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://first.example/openai/v1/",
            api_key="first-key",
        )
        second_llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://second.example/openai/v1/",
            api_key="second-key",
        )
        try:
            chain = RunnableParallel(first=first_llm, second=second_llm)
            with self.assertRaisesRegex(
                ValueError, "multiple values for secret OPENAI_API_KEY"
            ):
                _prepare_chain_for_worker(
                    chain,
                    subscription_key=None,
                    url=None,
                    api_version=None,
                )
        finally:
            first_llm.root_client.close()
            second_llm.root_client.close()

    def test_worker_reconstruction_rejects_query_parameters_in_url(self):
        llm = ChatOpenAI(
            model="gpt-5.1",
            base_url="https://old.openai.azure.com/openai/v1/",
            api_key="old-key",
        )
        try:
            with self.assertRaisesRegex(ValueError, "must not contain a query"):
                _prepare_chain_for_worker(
                    llm,
                    subscription_key="inline-key",
                    url="https://new.openai.azure.com/?api-version=ignored",
                    api_version="new-version",
                )
        finally:
            llm.root_client.close()

    def test_inline_openai_settings_override_openai_defaults(self):
        defaults = OpenAIDefaults()
        defaults.set_subscription_key("default-key")
        defaults.set_URL("https://default.openai.azure.com/")
        defaults.set_api_version("default-version")
        try:
            default_transformer = LangchainTransformer()
            self.assertEqual(
                default_transformer._get_effective_openai_settings(),
                (
                    "default-key",
                    "https://default.openai.azure.com/",
                    "default-version",
                    True,
                ),
            )

            inline_transformer = (
                LangchainTransformer()
                .setSubscriptionKey("inline-key")
                .setUrl("https://inline.openai.azure.com/")
                .setApiVersion("inline-version")
            )
            self.assertEqual(
                inline_transformer._get_effective_openai_settings(),
                (
                    "inline-key",
                    "https://inline.openai.azure.com/",
                    "inline-version",
                    True,
                ),
            )
        finally:
            defaults.reset_subscription_key()
            defaults.reset_URL()
            defaults.reset_api_version()


if __name__ == "__main__":
    unittest.main()
