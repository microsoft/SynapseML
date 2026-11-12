# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Opt-in live cache qualification; never treat missing provider counters as zero.

Set SYNAPSEML_LIVE_CACHE_TESTS=1, OPENAI_API_KEY, OPENAI_BASE_URL and
OPENAI_CACHE_MODEL to use an existing Azure OpenAI deployment. Foundry checks
also require AI_FOUNDRY_BASE_URL, AI_FOUNDRY_API_KEY, AI_FOUNDRY_OPENAI_MODEL
and AI_FOUNDRY_THIRD_PARTY_MODEL, naming two deployments on that endpoint.
Checks make immediate warm-up/read requests, without waiting for or testing
cache lifetime or eviction. Missing configuration skips the affected tests,
which does not qualify those providers.
SYNAPSEML_CACHE_EVIDENCE optionally names a JSONL file for safe usage evidence.
"""

import json
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


@unittest.skipUnless(
    os.environ.get("SYNAPSEML_LIVE_CACHE_TESTS") == "1",
    "Live cache qualification requires explicit opt-in and an existing deployment",
)
class TestOpenAICacheLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        required = ("OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_CACHE_MODEL")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise unittest.SkipTest(
                "Missing live cache settings: " + ", ".join(missing)
            )
        from synapse.ml.core.init_spark import init_spark

        cls.spark = init_spark()
        cls.base = os.environ["OPENAI_BASE_URL"].rstrip("/") + "/"
        cls.key = os.environ["OPENAI_API_KEY"]
        cls.model = os.environ["OPENAI_CACHE_MODEL"]

    def record(self, phase, **values):
        record = {
            "test": self.id(),
            "phase": phase,
            "utc": datetime.now(timezone.utc).isoformat(),
            **values,
        }
        path = os.environ.get("SYNAPSEML_CACHE_EVIDENCE")
        if path:
            with open(path, "a", encoding="utf-8") as stream:
                stream.write(json.dumps(record) + "\n")

    def prefix(self):
        return (
            "Synthetic qualification "
            + uuid.uuid4().hex
            + "\n"
            + "\n".join(
                f"Product {i}: warranty is two years; returns require a receipt "
                "within thirty days. Repairs require a serial number."
                for i in range(80)
            )
        )

    def messages(self, prefix, explicit=True, chat=False):
        part = {"type": "text" if chat else "input_text", "text": prefix}
        if explicit:
            part["prompt_cache_breakpoint"] = {"mode": "explicit"}
        return [
            {"role": "developer", "content": [part]},
            {
                "role": "user",
                "content": [
                    {
                        "type": "text" if chat else "input_text",
                        "text": "Reply only OK.",
                    }
                ],
            },
        ]

    def native(self, phase, body, chat=False, foundry=False):
        if foundry:
            base = os.environ["AI_FOUNDRY_BASE_URL"].rstrip("/") + "/"
            key = os.environ["AI_FOUNDRY_API_KEY"]
            route = "models/chat/completions?api-version=2024-05-01-preview"
        else:
            base, key = self.base, self.key
            route = "chat/completions" if chat else "responses"
        request = Request(
            base + route,
            data=json.dumps(body).encode("utf-8"),
            headers={"api-key": key, "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=120) as response:
                result = json.load(response)
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            self.record(phase, http_status=error.code, provider_error=detail)
            self.fail(f"{phase}: HTTP {error.code}: {detail}")
        self.record(
            phase,
            usage=result.get("usage"),
            status=result.get("status"),
            response_id=result.get("id"),
            model=result.get("model"),
            output_items=len(result.get("output", [])),
        )
        self.assertIsNone(result.get("error"), result.get("error"))
        return result

    def responses_body(self, prefix, options):
        return {
            "model": self.model,
            "input": self.messages(prefix),
            "reasoning": {"effort": "low"},
            "text": {"verbosity": "low"},
            "store": False,
            "prompt_cache_options": options,
        }

    def details(self, response, chat=False):
        usage = response["usage"]
        field = "prompt_tokens_details" if chat else "input_tokens_details"
        details = usage.get(field)
        self.assertIsInstance(
            details, dict, f"Provider did not report {field}: {usage}"
        )
        for key in ("cached_tokens", "cache_write_tokens"):
            self.assertIn(key, details, f"Provider did not report {key}: {details}")
            self.assertIsInstance(details[key], int, (key, details))
            self.assertGreaterEqual(details[key], 0, (key, details))
        total = usage["prompt_tokens" if chat else "input_tokens"]
        self.assertLessEqual(
            details["cached_tokens"] + details["cache_write_tokens"], total
        )
        if not chat:
            by_ttl = details["cache_write_tokens_details"]
            self.assertGreaterEqual(by_ttl["ttl_30m_tokens"], 0)
            self.assertGreaterEqual(by_ttl["ttl_12h_tokens"], 0)
            self.assertEqual(
                by_ttl["ttl_30m_tokens"] + by_ttl["ttl_12h_tokens"],
                details["cache_write_tokens"],
            )
        return details

    def frame(self, prefix, options, chat=False):
        part = (
            "struct<type:string,text:string,"
            "prompt_cache_breakpoint:map<string,string>>"
        )
        return self.spark.createDataFrame(
            [(self.messages(prefix, chat=chat), options)],
            f"messages array<struct<role:string,content:array<{part}>>>, "
            "cache_options map<string,string>",
        )

    def stage(self, stage_type):
        return (
            stage_type()
            .setUrl(self.base)
            .setDeploymentName(self.model)
            .setMessagesCol("messages")
            .setReasoningEffort("low")
            .setVerbosity("low")
            .setOutputCol("output")
            .setErrorCol("error")
        )

    def collect(self, stage, frame, phase, prompt=False, key=None):
        # Set the credential only after save/load; never persist it with the model.
        rows = stage.setSubscriptionKey(key or self.key).transform(frame).collect()
        self.assertEqual(len(rows), 1)
        row = rows[0].asDict(recursive=True)
        self.record(
            phase,
            error=row["error"],
            usage=row.get("usage") if prompt else (row["output"] or {}).get("usage"),
        )
        self.assertIsNone(row["error"], row["error"])
        self.assertIsNotNone(row["output"])
        return row if prompt else row["output"]

    def test_responses_scalar(self):
        from synapse.ml.services.openai.OpenAIResponses import OpenAIResponses

        prefix = self.prefix()
        options = {"mode": "explicit", "ttl": "30m"}
        cold = self.details(
            self.native("native-cold", self.responses_body(prefix, options))
        )
        self.assertGreater(cold["cache_write_tokens"], 0)
        stage = self.stage(OpenAIResponses).setPromptCacheOptions(options)
        warm = self.collect(stage.copy({}), self.frame(prefix, options), "scalar-copy")
        self.assertGreater(self.details(warm)["cached_tokens"], 0)

    def test_responses_reload(self):
        from synapse.ml.services.openai.OpenAIResponses import OpenAIResponses

        prefix = self.prefix()
        options = {"mode": "explicit", "ttl": "30m"}
        stage = self.stage(OpenAIResponses).setPromptCacheOptions(options)
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "responses")
            stage.save(path)
            restored = OpenAIResponses.load(path)
            cold = self.details(
                self.native("reload-native-cold", self.responses_body(prefix, options))
            )
            self.assertGreater(cold["cache_write_tokens"], 0)
            warm = self.collect(
                restored, self.frame(prefix, options), "restored-scalar"
            )
            self.assertGreater(self.details(warm)["cached_tokens"], 0)

    def test_responses_column_30m_and_12h(self):
        from synapse.ml.services.openai.OpenAIResponses import OpenAIResponses

        for ttl in ("30m", "12h"):
            with self.subTest(ttl=ttl):
                prefix = self.prefix()
                options = {"mode": "explicit", "ttl": ttl}
                stage = self.stage(OpenAIResponses).setPromptCacheOptionsCol(
                    "cache_options"
                )
                frame = self.frame(prefix, options)
                cold = self.details(self.collect(stage, frame, f"{ttl}-column-cold"))
                self.assertGreater(
                    cold["cache_write_tokens_details"][f"ttl_{ttl}_tokens"], 0
                )
                warm = self.details(self.collect(stage, frame, f"{ttl}-column-warm"))
                self.assertGreater(warm["cached_tokens"], 0)

    def test_responses_prewarm(self):
        from synapse.ml.services.openai.OpenAIResponses import OpenAIResponses

        prefix = self.prefix()
        options = {"mode": "explicit", "ttl": "30m", "prewarm": True}
        with self.subTest(client="native"):
            prewarmed = self.native(
                "native-prewarm", self.responses_body(prefix, options)
            )
            self.assertEqual(prewarmed["output"], [])
            self.assertEqual(prewarmed["usage"]["output_tokens"], 0)
            self.assertGreater(self.details(prewarmed)["cache_write_tokens"], 0)
        stage = self.stage(OpenAIResponses).setPromptCacheOptions(options)
        frame = self.frame(prefix, {"mode": "explicit", "ttl": "30m"})
        with self.subTest(client="synapseml"):
            prewarmed = self.collect(stage, frame, "prewarm")
            self.assertEqual(prewarmed["output"], [])
            self.assertEqual(prewarmed["usage"]["output_tokens"], 0)
            self.details(prewarmed)
            stage.setPromptCacheOptions({"mode": "explicit", "ttl": "30m"})
            warm = self.collect(stage, frame, "after-prewarm")
            self.assertGreater(self.details(warm)["cached_tokens"], 0)
            self.assertTrue(warm["output"])

    def test_prompt_implicit_usage_and_reload(self):
        from synapse.ml.services.openai.OpenAIPrompt import OpenAIPrompt

        stage = (
            self.stage(OpenAIPrompt)
            .setApiType("responses")
            .setSystemPrompt(self.prefix())
            .setPromptTemplate("{question}")
            .setPromptCacheOptions({"mode": "implicit", "ttl": "30m"})
            .setUsageCol("usage")
        )
        frame = self.spark.createDataFrame([("Reply only OK.",)], "question string")
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "prompt")
            stage.save(path)
            stage = OpenAIPrompt.load(path)
        cold = self.collect(stage, frame, "prompt-cold", prompt=True)
        warm = self.collect(stage, frame, "prompt-warm", prompt=True)
        cold_details = cold["usage"]["input_token_details"]
        warm_details = warm["usage"]["input_token_details"]
        self.assertGreater(cold_details["cache_write_tokens"], 0)
        self.assertGreater(warm_details["cached_tokens"], 0)
        for details in (cold_details, warm_details):
            self.assertEqual(
                details["cache_write_tokens_details.ttl_30m_tokens"]
                + details["cache_write_tokens_details.ttl_12h_tokens"],
                details["cache_write_tokens"],
            )

    def test_chat_cache_reads_and_writes(self):
        from synapse.ml.services.openai.OpenAIChatCompletion import OpenAIChatCompletion

        prefix = self.prefix()
        options = {"mode": "explicit"}
        body = {
            "model": self.model,
            "messages": self.messages(prefix, chat=True),
            "reasoning_effort": "low",
            "verbosity": "low",
            "prompt_cache_options": options,
        }
        cold = self.details(self.native("native-chat-cold", body, chat=True), chat=True)
        self.assertGreater(cold["cache_write_tokens"], 0)
        stage = self.stage(OpenAIChatCompletion).setPromptCacheOptions(options)
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "chat")
            stage.save(path)
            stage = OpenAIChatCompletion.load(path)
        warm = self.collect(
            stage, self.frame(prefix, options, chat=True), "restored-chat-warm"
        )
        self.assertGreater(self.details(warm, chat=True)["cached_tokens"], 0)

    @unittest.skipUnless(
        all(
            os.environ.get(name)
            for name in (
                "AI_FOUNDRY_BASE_URL",
                "AI_FOUNDRY_API_KEY",
                "AI_FOUNDRY_OPENAI_MODEL",
                "AI_FOUNDRY_THIRD_PARTY_MODEL",
            )
        ),
        "Foundry cache qualification needs an endpoint, credential, "
        "AI_FOUNDRY_OPENAI_MODEL and AI_FOUNDRY_THIRD_PARTY_MODEL",
    )
    def test_foundry_model_inference(self):
        from synapse.ml.services.aifoundry.AIFoundryChatCompletion import (
            AIFoundryChatCompletion,
        )

        for model in (
            os.environ["AI_FOUNDRY_OPENAI_MODEL"],
            os.environ["AI_FOUNDRY_THIRD_PARTY_MODEL"],
        ):
            with self.subTest(model=model):
                prefix = self.prefix()
                messages = [{"role": "user", "content": prefix + "\nReply only OK."}]
                body = {
                    "model": model,
                    "messages": messages,
                    "max_completion_tokens": 256,
                }
                self.native(f"foundry-{model}-baseline", body, chat=True, foundry=True)
                body["prompt_cache_options"] = {"mode": "implicit"}
                cold = self.native(
                    f"foundry-{model}-cache", body, chat=True, foundry=True
                )
                frame = self.spark.createDataFrame(
                    [(messages,)], "messages array<struct<role:string,content:string>>"
                )
                stage = (
                    AIFoundryChatCompletion()
                    .setUrl(os.environ["AI_FOUNDRY_BASE_URL"])
                    .setApiVersion("2024-05-01-preview")
                    .setModel(model)
                    .setMessagesCol("messages")
                    .setMaxCompletionTokens(256)
                    .setPromptCacheOptions({"mode": "implicit"})
                    .setOutputCol("output")
                    .setErrorCol("error")
                )
                self.collect(
                    stage,
                    frame,
                    f"foundry-{model}-first",
                    key=os.environ["AI_FOUNDRY_API_KEY"],
                )
                warm = self.collect(
                    stage,
                    frame,
                    f"foundry-{model}-warm",
                    key=os.environ["AI_FOUNDRY_API_KEY"],
                )
                self.assertGreater(self.details(warm, chat=True)["cached_tokens"], 0)
                self.details(cold, chat=True)
