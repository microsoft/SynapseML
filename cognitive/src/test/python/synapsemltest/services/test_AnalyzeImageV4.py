# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
from pathlib import Path
import shutil
import threading
import unittest
from uuid import uuid4
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.vision.AnalyzeImageV4 import AnalyzeImageV4
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

spark = init_spark()


class TestAnalyzeImageV4(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.request_keys = []
        requests = self.requests
        request_keys = self.request_keys

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                uri = urlsplit(self.path)
                body = self.rfile.read(int(self.headers["Content-Length"]))
                requests.append(
                    (uri.path, parse_qs(uri.query), self.headers["Content-Type"], body)
                )
                request_keys.append(self.headers.get("Ocp-Apim-Subscription-Key"))
                if uri.path == "/error":
                    status = 400
                    response = {"error": {"code": "InvalidRequest"}}
                else:
                    status = 200
                    response = {
                        "modelVersion": "2023-10-01",
                        "metadata": {"width": 100, "height": 50},
                        "tagsResult": {"values": [{"name": "cat", "confidence": 0.99}]},
                        "readResult": {
                            "blocks": [
                                {
                                    "lines": [
                                        {
                                            "text": text,
                                            "boundingPolygon": [],
                                            "words": [],
                                        }
                                    ]
                                }
                                for text in ["hello", "world"]
                            ]
                        },
                    }
                payload = json.dumps(response).encode("utf-8")
                if uri.path == "/malformed":
                    payload = b"{not-json"
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = "http://127.0.0.1:{}".format(self.server.server_port)
        self.data = spark.createDataFrame(
            [("https://example.org/image.jpg",), (None,)], ["image"]
        ).coalesce(1)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def stage(self):
        return (
            AnalyzeImageV4()
            .setEndpoint(self.endpoint)
            .setImageUrlCol("image")
            .setFeatures(["tags", "read"])
            .setOutputCol("response")
            .setErrorCol("error")
            .setConcurrency(1)
        )

    def test_v4_generated_parameters_response_and_null_input(self):
        stage = (
            self.stage()
            .setGenderNeutralCaption(True)
            .setSmartCropsAspectRatios([0.75, 1.8])
        )
        output = stage.transform(self.data)
        rows = output.collect()
        self.assertIsNone(rows[0].error)
        self.assertEqual(rows[0].response.tagsResult.values[0].name, "cat")
        self.assertEqual(
            [
                line.text
                for block in rows[0].response.readResult.blocks
                for line in block.lines
            ],
            ["hello", "world"],
        )
        self.assertIsNone(rows[0].response.objectsResult)
        self.assertIsNone(rows[1].response)
        self.assertIsNone(rows[1].error)
        self.assertNotIn("description", output.schema["response"].dataType.fieldNames())
        self.assertEqual(len(self.requests), 1)
        path, query, content_type, body = self.requests[0]
        self.assertEqual(path, "/computervision/imageanalysis:analyze")
        self.assertEqual(
            query,
            {
                "api-version": ["2024-02-01"],
                "features": ["tags,read"],
                "gender-neutral-caption": ["true"],
                "smartcrops-aspect-ratios": ["0.75,1.8"],
            },
        )
        self.assertTrue(content_type.startswith("application/json"))
        self.assertEqual(json.loads(body), {"url": self.data.head().image})

    def test_url_reset_copy_save_and_load_keep_python_and_java_in_sync(self):
        stage = (
            self.stage()
            .setSmartCropsAspectRatios([1, 1.5])
            .setUrl(self.endpoint + "/stale")
        )
        stage.setLocation("eastus")
        self.assertEqual(
            stage.getUrl(),
            "https://eastus.api.cognitive.microsoft.com/computervision/imageanalysis:analyze",
        )
        stage.setCustomServiceName("example")
        self.assertEqual(
            stage.getUrl(),
            "https://example.cognitiveservices.azure.com/computervision/imageanalysis:analyze",
        )
        stage.setEndpoint(self.endpoint + "/")
        expected = stage.transform(self.data).collect()
        self.assertEqual(self.requests[-1][0], "/computervision/imageanalysis:analyze")
        stage.setUrl(self.endpoint + "/explicit?api-version=preview&trace=a%26b%20c")
        directory = Path("vision-v4-python-{}".format(uuid4().hex))
        try:
            stage.write().save(str(directory))
            for restored in (stage.copy(), AnalyzeImageV4.load(str(directory))):
                self.assertIsInstance(restored, AnalyzeImageV4)
                self.assertEqual(restored.transform(self.data).collect(), expected)
                self.assertEqual(self.requests[-1][0], "/explicit")
                self.assertEqual(
                    self.requests[-1][1],
                    {
                        "api-version": ["2024-02-01"],
                        "features": ["tags,read"],
                        "trace": ["a&b c"],
                        "smartcrops-aspect-ratios": ["1.0,1.5"],
                    },
                )
                restored.setEndpoint(self.endpoint)
                restored.transform(self.data).collect()
                self.assertEqual(
                    self.requests[-1][0], "/computervision/imageanalysis:analyze"
                )
        finally:
            if directory.exists():
                shutil.rmtree(directory)

    def test_vector_features_byte_input_and_invalid_features(self):
        schema = StructType(
            [
                StructField("image", StringType()),
                StructField("features", ArrayType(StringType())),
            ]
        )
        data = spark.createDataFrame(
            [("https://example.org/image.jpg", ["read"]), (None, ["read"])], schema
        ).coalesce(1)
        self.stage().setFeaturesCol("features").transform(data).collect()
        self.assertEqual(self.requests[-1][1]["features"], ["read"])
        stage = (
            AnalyzeImageV4()
            .setEndpoint(self.endpoint)
            .setImageBytesCol("image")
            .setOutputCol("response")
        )
        stage.transform(
            spark.createDataFrame([(bytearray([1, 2, 3]),)], ["image"])
        ).collect()
        self.assertEqual(self.requests[-1][2], "application/octet-stream")
        self.assertEqual(self.requests[-1][3], b"\x01\x02\x03")
        self.assertEqual(self.requests[-1][1]["features"], ["tags"])
        with self.assertRaises(Exception):
            self.stage().setFeatures(["Categories"])
        for invalid in (["1"], [True], [None], [0.5], [2], [float("nan")]):
            with self.subTest(ratios=invalid):
                with self.assertRaises(Exception):
                    self.stage().setSmartCropsAspectRatios(invalid)
        constructed = AnalyzeImageV4(smartCropsAspectRatios=[1, 1.5])
        self.assertEqual(constructed.getSmartCropsAspectRatios(), [1.0, 1.5])
        constructed.setParams(smartCropsAspectRatios=[1.25, 1])
        self.assertEqual(constructed.getSmartCropsAspectRatios(), [1.25, 1.0])

    def test_linked_service_replaces_prior_key_and_url_through_persistence(self):
        linked_url = self.endpoint + "/computervision/imageanalysis:analyze"
        linked_key = "linked-fixture-key"
        resolved_names = []

        class LinkedServiceBoundary:
            def __init__(self, java_stage):
                self.java_stage = java_stage

            def setLinkedService(self, name):
                resolved_names.append(name)
                return self.java_stage.setUrl(linked_url).setSubscriptionKey(linked_key)

        cached = AnalyzeImageV4()
        cached.set(cached.subscriptionKey, "old-fixture-key")
        stages = {
            "setter": AnalyzeImageV4().setSubscriptionKey("old-fixture-key"),
            "constructor": AnalyzeImageV4(subscriptionKey="old-fixture-key"),
            "setParams": AnalyzeImageV4().setParams(subscriptionKey="old-fixture-key"),
            "column": AnalyzeImageV4().setSubscriptionKeyCol("oldKey"),
            "paramMap": cached,
        }
        for name, stage in stages.items():
            with self.subTest(configuration=name):
                stage.setUrl(self.endpoint + "/stale").setImageUrlCol(
                    "image"
                ).setOutputCol("response").setErrorCol("error")
                # Replace only the platform resolver, keeping the real JVM stage
                # and production Python transfer/copy/persistence code.
                stage._java_obj = LinkedServiceBoundary(stage._java_obj)
                self.assertIs(stage.setLinkedService("fixture-service"), stage)
                stage._transfer_params_to_java()
                directory = Path("vision-v4-linked-{}".format(uuid4().hex))
                try:
                    stage.write().save(str(directory))
                    for restored in (
                        stage,
                        stage.copy(),
                        AnalyzeImageV4.load(str(directory)),
                    ):
                        self.assertIsInstance(restored, AnalyzeImageV4)
                        row = restored.transform(self.data).head()
                        self.assertIsNone(row.error)
                        self.assertEqual(row.response.tagsResult.values[0].name, "cat")
                        self.assertEqual(restored.getUrl(), linked_url)
                        self.assertEqual(restored.getSubscriptionKey(), linked_key)
                        self.assertEqual(
                            self.requests[-1][0],
                            "/computervision/imageanalysis:analyze",
                        )
                        self.assertEqual(self.request_keys[-1], linked_key)
                    stage.setSubscriptionKeyCol("key")
                    self.assertEqual(stage.getSubscriptionKeyCol(), "key")
                    data = spark.createDataFrame(
                        [("https://example.org/image.jpg", "column-fixture-key")],
                        ["image", "key"],
                    )
                    stage.transform(data).collect()
                    self.assertEqual(self.request_keys[-1], "column-fixture-key")
                finally:
                    if directory.exists():
                        shutil.rmtree(directory)
        self.assertEqual(resolved_names, ["fixture-service"] * len(stages))

    def test_numeric_crop_ratio_columns_are_normalized_for_requests(self):
        data = spark.createDataFrame(
            [("https://example.org/image.jpg", [1])], ["image", "ratios"]
        )
        self.assertEqual(data.schema["ratios"].dataType.simpleString(), "array<bigint>")
        stage = self.stage().setSmartCropsAspectRatiosCol("ratios")
        for element_type in ("bigint", "int", "float", "double"):
            with self.subTest(element_type=element_type):
                typed = data.withColumn(
                    "ratios", col("ratios").cast("array<{}>".format(element_type))
                )
                row = stage.transform(typed).head()
                self.assertIsNone(row.error)
                self.assertEqual(row.response.tagsResult.values[0].name, "cat")
                self.assertEqual(
                    self.requests[-1][1]["smartcrops-aspect-ratios"], ["1.0"]
                )
        fractional = spark.createDataFrame(
            [("https://example.org/image.jpg", [0.75, 1.5])], ["image", "ratios"]
        )
        for element_type in ("float", "double"):
            typed = fractional.withColumn(
                "ratios", col("ratios").cast("array<{}>".format(element_type))
            )
            stage.transform(typed).collect()
            self.assertEqual(
                self.requests[-1][1]["smartcrops-aspect-ratios"], ["0.75,1.5"]
            )
        self.assertEqual(len(self.requests), 6)

    def test_invalid_crop_ratio_columns_fail_validation_before_http(self):
        stage = self.stage().setSmartCropsAspectRatiosCol("ratios")
        cases = [
            ("double", []),
            ("double", [None]),
            ("double", [float("nan")]),
            ("double", [float("inf")]),
            ("double", [0.5]),
            ("bigint", [2]),
            ("string", ["1"]),
            ("boolean", [True]),
        ]
        for element_type, ratios in cases:
            with self.subTest(element_type=element_type, ratios=ratios):
                data = spark.createDataFrame(
                    [("https://example.org/image.jpg", ratios)],
                    "image string, ratios array<{}>".format(element_type),
                )
                with self.assertRaises(Exception) as error:
                    stage.transform(data).collect()
                self.assertIn("Invalid smart crop aspect ratios", str(error.exception))
                self.assertNotIn("ClassCastException", str(error.exception))
                self.assertNotIn("NullPointerException", str(error.exception))
                self.assertEqual(len(self.requests), 0)
        optional = spark.createDataFrame(
            [("https://example.org/image.jpg", None)],
            "image string, ratios array<double>",
        )
        stage.transform(optional).collect()
        self.assertNotIn("smartcrops-aspect-ratios", self.requests[-1][1])

    def test_http_error_and_malformed_success_are_distinct(self):
        stage = self.stage().setUrl(self.endpoint + "/error")
        rows = stage.transform(self.data).collect()
        self.assertIsNone(rows[0].response)
        self.assertEqual(rows[0].error.status.statusCode, 400)
        self.assertEqual(
            json.loads(rows[0].error.response)["error"]["code"], "InvalidRequest"
        )
        self.assertIsNone(rows[1].error)
        row = stage.setUrl(self.endpoint + "/malformed").transform(self.data).head()
        self.assertIsNone(row.error)
        self.assertTrue(all(value is None for value in row.response))
        self.assertEqual(len(self.requests), 2)


if __name__ == "__main__":
    unittest.main()
