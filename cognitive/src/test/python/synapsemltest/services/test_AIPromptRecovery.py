# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from pyspark.errors import IllegalArgumentException
from pyspark.sql import DataFrame
from synapse.ml.services.ai import AIPrompt, AISystemOne
from synapse.ml.services.openai import OpenAIDecisions
from synapsemltest.services.test_AIPrompt import endpoint, spark


class TestAIPromptRecoveryConfirmation(unittest.TestCase):
    def test_administrative_release_requires_explicit_stopped_confirmation(self):
        with self.assertRaisesRegex(ValueError, "Stop the previous Spark application"):
            AIPrompt.releaseOutputOwner(spark, "unused", "unused")


@unittest.skipUnless(
    os.environ.get("AIPROMPT_RUN_PYTHON_DELTA_TESTS") == "true",
    "Requires caller-configured Delta and the cognitive test JAR",
)
class TestAIPromptDurable(unittest.TestCase):
    def test_generated_durable_methods_recover_without_replaying_committed_rows(self):
        cases = (
            (
                AIPrompt,
                {
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "answer"},
                        }
                    ]
                },
            ),
            (
                AISystemOne,
                {"model": "model", "answers": {"check": {"result": True}}},
            ),
            (
                OpenAIDecisions,
                {
                    "model": "model",
                    "answers": [
                        {"type": "predicate", "name": "check", "probability": 0.9}
                    ],
                },
            ),
        )
        texts = ['{"note":"first"}', '{"note":"second"}', '{"note":"third"}']
        data = spark.createDataFrame([(text,) for text in texts], "text STRING")
        numeric_text = (
            '{"trailing":2.50,"integer":1.00,"exponent":1e2,"zero":-0.00,'
            '"precise":0.12345678901234567890123456789,"tiny":1e-400,"huge":1e400}'
        )
        numbers = json.loads(numeric_text, parse_float=Decimal)
        for stage_type, reply in cases:
            with self.subTest(stage=stage_type.__name__), endpoint(
                reply, exact_numbers=True
            ) as (
                url,
                requests,
            ), tempfile.TemporaryDirectory() as directory:
                root = str(Path(directory) / "run")

                def configured():
                    stage = stage_type(
                        url=url if stage_type is AISystemOne else url + "/v1",
                        model="model",
                        outputCol="answer",
                        errorCol="error",
                    )
                    stage.setExtraBody('{"numeric":' + numeric_text + "}")
                    if stage_type is not AISystemOne:
                        stage.setInputCol("text")
                    if stage_type is AIPrompt:
                        stage.setResponseSchema(
                            {
                                "type": "object",
                                "properties": {"value": {"type": "number"}},
                                "numeric": numbers,
                            }
                        )
                    elif stage_type is AISystemOne:
                        stage.setQuestions(
                            {
                                "check": {
                                    "type": "noul",
                                    "instructions": "Check",
                                    "numeric": numbers,
                                }
                            }
                        )
                        stage.setStateCol("text").setStateMode("json")
                        self.assertEqual(stage.getInputMode(), "json")
                    elif stage_type is OpenAIDecisions:
                        stage.setQuestions(
                            [
                                {
                                    "type": "predicate",
                                    "name": "check",
                                    "instructions": "Check",
                                    "numeric": numbers,
                                }
                            ]
                        )
                        stage.setStateMode("text")
                        self.assertEqual(stage.getInputMode(), "text")
                    return stage

                original = configured().setOutputPath(root).setOutputBatchSize(1)
                original._transfer_params_to_java()
                spark._jvm.com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricRecovery.stopAfterFirstBatch(
                    original._java_obj, data._jdf
                )
                partial = stage_type.getOutputStatus(spark, root)
                self.assertIsInstance(partial, dict)
                self.assertEqual(partial["state"], "running")
                self.assertEqual(
                    [
                        partial[key]
                        for key in ("inputCount", "committedCount", "pendingCount")
                    ],
                    [3, 1, 2],
                )
                self.assertIs(partial["isComplete"], False)
                self.assertIs(partial["allSucceeded"], False)
                owner = partial["ownerToken"]
                self.assertIsInstance(owner, str)
                self.assertTrue(owner)
                committed = stage_type.loadOutput(spark, root)
                self.assertIsInstance(committed, DataFrame)
                self.assertEqual(committed.count(), 1)
                self.assertEqual(len(requests), 1)
                with self.assertRaisesRegex(
                    IllegalArgumentException, "ownership conflict"
                ):
                    stage_type.releaseOutputOwner(
                        spark, root, owner + "-wrong", confirmStopped=True
                    )
                self.assertEqual(
                    stage_type.getOutputStatus(spark, root)["ownerToken"], owner
                )
                stage_type.releaseOutputOwner(spark, root, owner, confirmStopped=True)
                self.assertIsNone(stage_type.getOutputStatus(spark, root)["ownerToken"])
                fresh = configured().setParams(outputPath=root, outputBatchSize=2)
                parameters = (
                    "ExtraBody",
                    "ResponseFormat" if stage_type is AIPrompt else "Questions",
                )
                for suffix in parameters:
                    value = getattr(original, "get" + suffix)()
                    getattr(fresh, "set" + suffix)(value)
                    name = suffix[0].lower() + suffix[1:]
                    parameter = original._java_obj.getParam(name)
                    self.assertNotEqual(
                        parameter.jsonEncode(
                            original._java_obj.getOrDefault(parameter)
                        ),
                        parameter.jsonEncode(
                            fresh._java_obj.getOrDefault(fresh._java_obj.getParam(name))
                        ),
                    )
                self.assertIsInstance(
                    fresh.getExtraBody()["numeric"]["trailing"], float
                )
                self.assertIsInstance(
                    fresh.getExtraBody()["numeric"]["exponent"], float
                )
                resumed = fresh.resume(spark)
                self.assertIsInstance(resumed, DataFrame)
                rows = resumed.orderBy("text").collect()
                self.assertEqual([row.text for row in rows], sorted(texts))
                self.assertTrue(
                    all(row.error is None and row.answer is not None for row in rows)
                )
                self.assertEqual(
                    stage_type.loadOutput(spark, root).orderBy("text").collect(), rows
                )
                self.assertEqual(fresh.resume(spark).orderBy("text").collect(), rows)
                self.assertEqual(original.resume(spark).orderBy("text").collect(), rows)
                complete = stage_type.getOutputStatus(spark, root)
                self.assertEqual(complete["runId"], partial["runId"])
                self.assertEqual(complete["state"], "complete")
                self.assertEqual(complete["committedCount"], 3)
                self.assertEqual(complete["pendingCount"], 0)
                self.assertEqual(complete["rowErrorCount"], 0)
                self.assertIsNone(complete["ownerToken"])
                self.assertIs(complete["isComplete"], True)
                self.assertIs(complete["allSucceeded"], True)
                self.assertGreater(
                    complete["resultsVersion"], partial["resultsVersion"]
                )
                self.assertEqual(len(requests), 3)
                self.assertTrue(all(body["numeric"] == numbers for _, body in requests))


if __name__ == "__main__":
    unittest.main()
