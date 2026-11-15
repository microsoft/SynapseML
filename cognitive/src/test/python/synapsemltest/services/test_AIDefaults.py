# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import tempfile
import unittest
from pathlib import Path

from pyspark.errors import IllegalArgumentException
from synapse.ml.services.ai import AIPrompt
from synapse.ml.services.ai.AIDefaults import AIDefaults
from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults
from synapsemltest.services.test_AIPrompt import endpoint, spark


class TestAIDefaults(unittest.TestCase):
    def setUp(self):
        self.defaults = AIDefaults()
        self.legacy = OpenAIDefaults()
        self.fields = (
            "URL",
            "subscription_key",
            "deployment_name",
            "model",
            "api_version",
            "api_type",
            "embedding_deployment_name",
            "temperature",
            "top_p",
            "seed",
            "verbosity",
            "reasoning_effort",
        )
        self.previous = {
            field: getattr(self.legacy, "get_" + field)() for field in self.fields
        }
        self.defaults.reset_all()

    def tearDown(self):
        self.defaults.reset_all()
        for field, value in self.previous.items():
            if value is not None:
                getattr(self.legacy, "set_" + field)(value)

    def test_legacy_and_new_facades_share_jvm_state_and_resets(self):
        shared = self.defaults.for_api("openai")
        self.legacy.set_URL("https://legacy.example.invalid/v1")
        self.legacy.set_subscription_key("synthetic-key")
        self.assertEqual(shared.get_endpoint(), self.legacy.get_URL())
        self.assertEqual(shared.get_api_key(), self.legacy.get_subscription_key())
        shared.set_model("new-model").set_temperature(0.25).set_top_p(0.8)
        shared.set_seed(7).set_api_type("responses").set_reasoning_effort("high")
        shared.set_verbosity("low").set_api_version("preview")
        shared.set_embedding_deployment_name("embedding")
        for field in (
            "model",
            "temperature",
            "top_p",
            "seed",
            "api_type",
            "reasoning_effort",
            "verbosity",
            "api_version",
            "embedding_deployment_name",
        ):
            self.assertEqual(
                getattr(shared, "get_" + field)(),
                getattr(self.legacy, "get_" + field)(),
            )
        self.legacy.reset_model()
        shared.reset_api_key()
        self.assertIsNone(shared.get_model())
        self.assertIsNone(self.legacy.get_subscription_key())

    def test_public_prompt_uses_scoped_defaults_after_copy_and_load(self):
        reply = {
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "answer"},
                    "finish_reason": "stop",
                }
            ]
        }
        data = spark.createDataFrame([("hello",)], "text STRING")
        with endpoint(reply) as (url, requests), tempfile.TemporaryDirectory() as tmp:
            scope = self.defaults.for_api("chat_completions")
            scope.set_endpoint(url + "/v1").set_model("first")
            scope.set_api_key("synthetic-scoped")
            stage = AIPrompt(
                apiType="chat_completions",
                inputCol="text",
                outputCol="answer",
                errorCol="error",
            )
            path = str(Path(tmp) / "stage")
            stage.write().save(path)
            scope.set_model("second")
            for candidate in (stage, stage.copy({}), AIPrompt.load(path)):
                self.assertEqual(candidate.transform(data).first().answer, "answer")
                self.assertFalse(candidate.isSet(candidate.url))
                self.assertFalse(candidate.isSet(candidate.subscriptionKey))
            self.assertEqual([body["model"] for _, body in requests], ["second"] * 3)
            stage.setModel("explicit").transform(data).collect()
            self.assertEqual(requests[-1][1]["model"], "explicit")

    def test_content_maps_booleans_and_scope_isolation_cross_the_jvm_bridge(self):
        content = self.defaults.for_api("content_understanding")
        mappings = {"completion": "deployment"}
        content.set_endpoint("https://cu.example.invalid").set_analyzer_id(
            "prebuilt-read"
        )
        content.set_model_deployments(mappings).set_allow_input_truncation(False)
        mappings["completion"] = "changed"
        self.assertEqual(content.get_model_deployments(), {"completion": "deployment"})
        self.assertIs(content.get_allow_input_truncation(), False)
        content.set_model_deployments({})
        self.assertEqual(content.get_model_deployments(), {})
        self.assertIsNone(self.defaults.for_api("openai").get_endpoint())
        with self.assertRaises(IllegalArgumentException):
            content.set_model("unsupported")
        with self.assertRaises(TypeError):
            content.set_model_deployments({"completion": None})
        with self.assertRaises(TypeError):
            content.set_allow_input_truncation("false")
        content.reset_model_deployments()
        content.reset_allow_input_truncation()
        self.assertIsNone(content.get_model_deployments())
        self.assertIsNone(content.get_allow_input_truncation())

    def test_instance_auth_overrides_shared_key_with_model_only_scopes(self):
        replies = {
            "chat_completions": {
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "answer"},
                        "finish_reason": "stop",
                    }
                ]
            },
            "responses": {
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "answer"}],
                    }
                ],
            },
        }
        data = spark.createDataFrame(
            [("hello", "synthetic-column"), ("hello", None)],
            "text STRING, token STRING",
        )
        for api, reply in replies.items():
            headers = []
            with self.subTest(api=api), endpoint(reply, recorded_headers=headers) as (
                url,
                _,
            ):
                self.defaults.for_api("openai").set_endpoint(url + "/v1").set_api_key(
                    "synthetic-shared"
                )
                self.defaults.for_api(api).set_model("model")
                for configure in (
                    lambda stage: stage.setAADToken("synthetic-aad"),
                    lambda stage: stage.setCustomAuthHeader("Bearer synthetic-custom"),
                    lambda stage: stage.setAADTokenCol("token"),
                    lambda stage: stage.setCustomAuthHeaderCol("token"),
                ):
                    headers.clear()
                    prompt = configure(
                        AIPrompt(
                            apiType=api,
                            inputCol="text",
                            outputCol="answer",
                            errorCol="error",
                        )
                    )
                    rows = prompt.transform(data).collect()
                    self.assertTrue(all(row.answer == "answer" for row in rows))
                    self.assertEqual(len(headers), 2)
                    self.assertTrue(all("api-key" not in h for h in headers))
                    self.assertTrue(any("authorization" in h for h in headers))

    def test_content_understanding_public_transform_and_snapshot_when_installed(self):
        try:
            from synapse.ml.services.contentunderstanding import ContentUnderstanding
        except ModuleNotFoundError as error:
            if error.name != "synapse.ml.services.contentunderstanding":
                raise
            self.skipTest("Requires the separately shipped ContentUnderstanding stage")
        reply = {"status": "Succeeded", "result": {"contents": []}}
        data = spark.createDataFrame(
            [("https://example.invalid/doc.pdf", {"X-Correlation": "example"})],
            "url STRING, headers MAP<STRING, STRING>",
        )
        with endpoint(reply) as (url, requests), tempfile.TemporaryDirectory() as tmp:
            scope = self.defaults.for_api("content_understanding")
            scope.set_endpoint(url).set_api_key("synthetic-content")
            scope.set_api_version("2026-06-01-preview").set_analyzer_id("prebuilt-read")
            scope.set_model_deployments({"completion": "cu-deployment"})
            scope.set_allow_input_truncation(False)
            original = (
                ContentUnderstanding()
                .setDocumentUrlCol("url")
                .setCustomHeadersCol("headers")
                .setOutputCol("analysis")
                .setErrorCol("error")
            )
            configured = scope.apply_to(original)
            self.assertFalse(original.isSet(original.url))
            self.assertFalse(original.isSet(original.subscriptionKey))
            self.assertEqual(configured.getSubscriptionKey(), "synthetic-content")
            self.assertEqual(configured.getAnalyzerId(), "prebuilt-read")
            scope.set_analyzer_id("different")
            saved = str(Path(tmp) / "content")
            configured.write().save(saved)
            loaded = ContentUnderstanding.load(saved)
            for candidate in (configured, loaded):
                row = candidate.transform(data).first()
                self.assertEqual(row.analysis.status, "Succeeded")
                self.assertIsNone(row.error)
            for path, body in requests:
                self.assertIn("/prebuilt-read:analyze", path)
                self.assertIn("api-version=2026-06-01-preview", path)
                self.assertIn("allowInputTruncation=false", path)
                self.assertEqual(
                    body["modelDeployments"], {"completion": "cu-deployment"}
                )
            overridden = scope.apply_to(original.setAnalyzerId("explicit"))
            self.assertEqual(overridden.getAnalyzerId(), "explicit")
            with endpoint(reply) as (other_url, _):
                overridden = scope.apply_to(original.setEndpoint(other_url))
                self.assertFalse(overridden.isSet(overridden.subscriptionKey))


if __name__ == "__main__":
    unittest.main()
