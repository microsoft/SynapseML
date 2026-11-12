# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Opt-in inference with synthetic data and environment-only credentials.

Generative tests use SYNAPSEML_GENERATIVE_{ENDPOINT,V1_ENDPOINT,DEPLOYMENT}
and TOKEN or KEY. ENDPOINT exercises facade resource-root routing; V1_ENDPOINT
is the explicit v1 base used by the standalone classes. Embedding tests use
SYNAPSEML_EMBEDDING_{ENDPOINT,DEPLOYMENT} and TOKEN or KEY, with an explicit
v1 base and a deployment supporting the dimensions parameter.
"""

import json
import math
import os
import unittest

from pyspark.ml.linalg import Vector
from pyspark.sql import SparkSession
from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.ai import AIPrompt, AISystemOne
from synapse.ml.services.openai import (
    OpenAIChatCompletion,
    OpenAIDecisions,
    OpenAIEmbedding,
    OpenAIResponses,
)


SYSTEM_QUESTIONS = {
    "repeat_contact": {
        "type": "noul",
        "instructions": "Has the customer contacted support before?",
    },
    "team": {
        "type": "choice",
        "instructions": "Which team should handle this request?",
        "criteria": {
            "billing": "Charges, invoices and refunds",
            "engineering": "Bugs, errors and outages",
        },
    },
    "severity": {
        "type": "score",
        "instructions": "How severe is this issue?",
        "criteria": ["Cosmetic", "Major", "Critical"],
    },
}
DECISION_QUESTIONS = [
    {
        "type": "predicate",
        "name": "repeat_contact",
        "instructions": "Has the customer contacted support before?",
    },
    {
        "type": "choice",
        "name": "team",
        "instructions": "Which team should handle this request?",
        "choices": [
            {"value": "billing", "description": "Charges, invoices and refunds"},
            {"value": "engineering", "description": "Bugs, errors and outages"},
        ],
    },
    {
        "type": "score",
        "name": "severity",
        "instructions": "How severe is this issue?",
        "levels": [
            {"label": "Cosmetic", "description": "Appearance only"},
            {"label": "Major", "description": "A workaround exists"},
            {"label": "Critical", "description": "A task is fully blocked"},
        ],
    },
]


class TestAIPromptLive(unittest.TestCase):
    def _run_generative(self, api):
        prefix = "SYNAPSEML_GENERATIVE"
        endpoint = os.environ.get(prefix + "_ENDPOINT", "").strip()
        deployment = os.environ.get(prefix + "_DEPLOYMENT", "").strip()
        token = os.environ.get(prefix + "_TOKEN", "").strip()
        key = os.environ.get(prefix + "_KEY", "").strip()
        if not (endpoint and deployment and (token or key)):
            self.skipTest(
                "Live generative endpoint, deployment and credentials are not configured"
            )
        spark = SparkSession.getActiveSession() or init_spark()
        dataset = spark.createDataFrame(
            [("France", 0.0, 100)], "country STRING, temperature DOUBLE, tokens INT"
        )
        stage = (
            AIPrompt()
            .setApiType(api)
            .setUrl(endpoint)
            .setDeploymentName(deployment)
            .setSystemPrompt("Return the country's capital as a JSON object.")
            .setPromptTemplate("What is the capital of {country}?")
            .setTemperatureCol("temperature")
            .setMaxTokensCol("tokens")
            .setResponseSchema(
                {
                    "type": "object",
                    "properties": {"capital": {"type": "string"}},
                    "required": ["capital"],
                    "additionalProperties": False,
                }
            )
            .setOutputCol("answer")
            .setErrorCol("error")
            .setUsageCol("usage")
            .setRawResponseCol("raw")
            .setConcurrency(1)
            .setTimeout(120)
        )
        if api == "responses":
            stage.setStore(False)
        if token:
            stage.setAADToken(token)
        else:
            stage.setSubscriptionKey(key)
        result = stage.transform(dataset)
        expected = stage._java_obj.transformSchema(dataset._jdf.schema()).json()
        self.assertEqual(json.loads(result.schema.json()), json.loads(expected))
        row = result.head()
        self.assertIsNone(row.error, "Live generative inference returned an error")
        self.assertEqual(json.loads(row.answer)["capital"], "Paris")
        self.assertIsNotNone(row.usage)
        raw = json.loads(row.raw)
        self.assertTrue(raw["model"])
        if api == "chat_completions":
            self.assertEqual(
                json.loads(raw["choices"][0]["message"]["content"]),
                json.loads(row.answer),
            )
        else:
            self.assertEqual(raw["status"], "completed")
            self.assertFalse(raw["store"])

    def test_live_chat_default_v1_structured_output(self):
        self._run_generative("chat_completions")

    def test_live_responses_default_v1_structured_output(self):
        self._run_generative("responses")

    def _run_direct_generative(self, api, stage_type):
        prefix = "SYNAPSEML_GENERATIVE"
        endpoint = os.environ.get(prefix + "_V1_ENDPOINT", "").strip()
        deployment = os.environ.get(prefix + "_DEPLOYMENT", "").strip()
        token = os.environ.get(prefix + "_TOKEN", "").strip()
        key = os.environ.get(prefix + "_KEY", "").strip()
        if not (endpoint and deployment and (token or key)):
            self.skipTest(
                "Live generative v1 endpoint, deployment and credentials are not configured"
            )
        spark = SparkSession.getActiveSession() or init_spark()
        dataset = spark.createDataFrame(
            [
                (
                    [
                        ("system", "Return the country's capital as a JSON object."),
                        ("user", "What is the capital of France?"),
                    ],
                    0.0,
                    100,
                )
            ],
            "messages ARRAY<STRUCT<role:STRING,content:STRING>>, "
            "temperature DOUBLE, tokens INT",
        )
        stage = (
            stage_type()
            .setUrl(endpoint)
            .setDeploymentName(deployment)
            .setMessagesCol("messages")
            .setTemperatureCol("temperature")
            .setMaxTokensCol("tokens")
            .setResponseSchema(
                {
                    "type": "object",
                    "properties": {"capital": {"type": "string"}},
                    "required": ["capital"],
                    "additionalProperties": False,
                }
            )
            .setOutputCol("answer")
            .setErrorCol("error")
            .setConcurrency(1)
            .setTimeout(120)
        )
        if api == "responses":
            stage.setStore(False)
        if token:
            stage.setAADToken(token)
        else:
            stage.setSubscriptionKey(key)
        result = stage.transform(dataset)
        expected = stage._java_obj.transformSchema(dataset._jdf.schema()).json()
        self.assertEqual(json.loads(result.schema.json()), json.loads(expected))
        row = result.head()
        self.assertIsNone(row.error, "Live standalone inference returned an error")
        self.assertTrue(row.answer.model)
        self.assertIsNotNone(row.answer.usage)
        if api == "chat_completions":
            answer = row.answer.choices[0].message.content
        else:
            self.assertEqual(row.answer.status, "completed")
            self.assertFalse(row.answer.store)
            answer = "".join(
                part.text
                for item in row.answer.output
                if item.type == "message"
                for part in item.content
                if part.type == "output_text"
            )
        self.assertEqual(json.loads(answer)["capital"], "Paris")

    def test_live_chat_standalone_structured_output(self):
        self._run_direct_generative("chat_completions", OpenAIChatCompletion)

    def test_live_responses_standalone_structured_output(self):
        self._run_direct_generative("responses", OpenAIResponses)

    def _run_embedding(self, direct):
        prefix = "SYNAPSEML_EMBEDDING"
        endpoint = os.environ.get(prefix + "_ENDPOINT", "").strip()
        deployment = os.environ.get(prefix + "_DEPLOYMENT", "").strip()
        token = os.environ.get(prefix + "_TOKEN", "").strip()
        key = os.environ.get(prefix + "_KEY", "").strip()
        if not (endpoint and deployment and (token or key)):
            self.skipTest(
                "Live embedding endpoint, deployment and credentials are not configured"
            )
        spark = SparkSession.getActiveSession() or init_spark()
        dataset = spark.createDataFrame(
            [(1, "The capital of France is Paris."), (2, None)],
            "id INT, text STRING",
        )
        stage = OpenAIEmbedding() if direct else AIPrompt().setApiType("embeddings")
        stage.setUrl(endpoint).setDeploymentName(deployment).setTextCol("text")
        stage.setDimensions(32).setOutputCol("embedding").setErrorCol("error")
        stage.setUsageCol("usage").setConcurrency(1).setTimeout(120)
        if not direct:
            stage.setRawResponseCol("raw")
        if token:
            stage.setAADToken(token)
        else:
            stage.setSubscriptionKey(key)
        result = stage.transform(dataset).cache()
        try:
            expected = stage._java_obj.transformSchema(dataset._jdf.schema()).json()
            self.assertEqual(json.loads(result.schema.json()), json.loads(expected))
            rows = {row.id: row for row in result.collect()}
            self.assertIsNone(
                rows[1].error, "Live embedding inference returned an error"
            )
            self.assertIsInstance(rows[1].embedding, Vector)
            self.assertEqual(len(rows[1].embedding), 32)
            self.assertTrue(all(math.isfinite(value) for value in rows[1].embedding))
            self.assertGreater(rows[1].embedding.norm(2), 0)
            self.assertIsNotNone(rows[1].usage)
            self.assertIsNone(rows[2].embedding)
            self.assertIsNone(rows[2].usage)
            if not direct:
                raw = json.loads(rows[1].raw)
                self.assertEqual(list(rows[1].embedding), raw["data"][0]["embedding"])
                self.assertEqual(
                    json.loads(rows[2].error.response)["code"], "missing_input"
                )
            pandas = result.toPandas().set_index("id")
            self.assertEqual(list(pandas.loc[1, "embedding"]), list(rows[1].embedding))
            self.assertIsNone(pandas.loc[2, "embedding"])
        finally:
            result.unpersist()

    def test_live_embedding_standalone_vector(self):
        self._run_embedding(direct=True)

    def test_live_embedding_facade_vector(self):
        self._run_embedding(direct=False)

    def _run_native(self, api, direct, prefix, model_setting, questions):
        def configured(name):
            return os.environ.get(prefix + name, "").strip()

        endpoint = configured("_ENDPOINT")
        model = configured(model_setting)
        token = configured("_TOKEN")
        key = configured("_KEY")
        if not (endpoint and model and (token or key)):
            self.skipTest(
                "Live native endpoint, model and credentials are not configured"
            )
        spark = SparkSession.getActiveSession() or init_spark()
        dataset = spark.createDataFrame(
            [
                (
                    1,
                    "Checkout is down again for every customer. "
                    "This is my third request.",
                ),
                (2, None),
            ],
            "id INT, ticket STRING",
        )
        for stage in (direct(), AIPrompt().setApiType(api)):
            with self.subTest(stage=type(stage).__name__):
                stage.setUrl(endpoint).setModel(model).setQuestions(questions)
                stage.setOutputCol("decision").setErrorCol("error")
                stage.setUsageCol("usage").setRawResponseCol("raw").setConcurrency(2)
                stage.setTimeout(120)
                if api == "systemone":
                    stage.setStateCol("ticket")
                else:
                    stage.setInputCol("ticket")
                if token:
                    stage.setAADToken(token)
                else:
                    stage.setSubscriptionKey(key)
                result = stage.transform(dataset).cache()
                try:
                    expected = stage._java_obj.transformSchema(
                        dataset._jdf.schema()
                    ).json()
                    self.assertEqual(
                        json.loads(result.schema.json()), json.loads(expected)
                    )
                    rows = {row.id: row for row in result.collect()}
                    self.assertIsNone(
                        rows[1].error, "Live native inference returned an error"
                    )
                    self.assertIsNotNone(rows[1].usage)
                    self.assertTrue(rows[1].decision.model)
                    raw = json.loads(rows[1].raw)
                    answers = rows[1].decision.answers
                    if api == "systemone":
                        self.assertEqual(set(answers), set(SYSTEM_QUESTIONS))
                        self.assertEqual(
                            {
                                name: json.loads(value)
                                for name, value in answers.items()
                            },
                            raw["answers"],
                        )
                    else:
                        self.assertEqual(
                            [answer.name for answer in answers],
                            [question["name"] for question in DECISION_QUESTIONS],
                        )
                        self.assertEqual(
                            [answer.type for answer in answers],
                            ["predicate", "choice", "score"],
                        )
                        self.assertGreaterEqual(answers[0].probability, 0)
                        self.assertLessEqual(answers[0].probability, 1)
                        self.assertEqual(answers[1].choice.type, "string")
                        self.assertIn(
                            answers[1].choice.stringValue, ("billing", "engineering")
                        )
                        self.assertGreaterEqual(answers[2].score, 0)
                        self.assertLessEqual(answers[2].score, 2)
                    self.assertIsNone(rows[2].decision)
                    self.assertIsNone(rows[2].usage)
                    self.assertEqual(
                        json.loads(rows[2].error.response)["code"], "missing_input"
                    )
                    # Reuse the materialized result: a second action must not call the model.
                    pandas = result.toPandas().set_index("id")
                    self.assertEqual(list(pandas.columns), result.columns[1:])
                    self.assertEqual(json.loads(pandas.loc[1, "raw"]), raw)
                    self.assertIsNone(pandas.loc[2, "decision"])
                finally:
                    result.unpersist()

    def test_live_systemone_pyspark_and_pandas_outputs(self):
        self._run_native(
            "systemone",
            AISystemOne,
            "SYNAPSEML_SYSTEMONE",
            "_DEPLOYMENT",
            SYSTEM_QUESTIONS,
        )

    def test_live_decisions_pyspark_and_pandas_outputs(self):
        self._run_native(
            "decisions",
            OpenAIDecisions,
            "SYNAPSEML_DECISIONS",
            "_MODEL",
            DECISION_QUESTIONS,
        )


if __name__ == "__main__":
    unittest.main()
