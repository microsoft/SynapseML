# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import base64
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.contentunderstanding import ContentUnderstanding

spark = init_spark()


class TestContentUnderstanding(unittest.TestCase):
    def setUp(self):
        self.requests = []
        requests = self.requests

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def send_json(self, status, body, location=None):
                payload = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                if location:
                    self.send_header("Operation-Location", location)
                self.end_headers()
                self.wfile.write(payload)

            def read_body(self):
                return json.loads(self.rfile.read(int(self.headers["Content-Length"])))

            def do_POST(self):
                requests.append(("POST", self.path, self.read_body()))
                operation = "123e4567-e89b-12d3-a456-426614174000"
                location = (
                    f"http://127.0.0.1:{self.server.server_port}"
                    f"/contentunderstanding/analyzerResults/{operation}"
                    "?api-version=2025-11-01"
                )
                self.send_json(
                    202,
                    {"id": operation, "status": "Running", "result": {"contents": []}},
                    location,
                )

            def do_GET(self):
                requests.append(("GET", self.path, None))
                if "/analyzerResults/" in self.path:
                    body = {
                        "id": "123e4567-e89b-12d3-a456-426614174000",
                        "status": "Succeeded",
                        "usage": {"tokens": {"future-model-input": 3}},
                        "result": {
                            "contents": [
                                {
                                    "markdown": "synthetic result",
                                    "fields": {"Optional": {"type": "string"}},
                                }
                            ]
                        },
                    }
                else:
                    body = {
                        "analyzerId": urlsplit(self.path).path.split("/")[-1],
                        "status": "ready",
                    }
                self.send_json(200, body)

            def do_PUT(self):
                requests.append(("PUT", self.path, self.read_body()))
                self.send_json(
                    201,
                    {
                        "analyzerId": urlsplit(self.path).path.split("/")[-1],
                        "status": "ready",
                    },
                )

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def analyzer(self):
        return (
            ContentUnderstanding()
            .setEndpoint(f"http://127.0.0.1:{self.server.server_port}")
            .setSubscriptionKey("synthetic-test-key")
            .setDocumentBytesCol("body")
            .setMimeType("text/plain")
            .setDocumentNameCol("documentId")
            .setOutputCol("analysis")
            .setMaxPollAttempts(2)
            .setPollingDelay(0)
        )

    def documents(self):
        return spark.createDataFrame(
            [("one", bytearray("Synthetic caf\u00e9 document".encode("utf-8")))],
            "documentId string, body binary",
        )

    def test_generated_wrapper_copy_and_persistence_preserve_request_options(self):
        analyzer = (
            self.analyzer()
            .setModelDeployments({"completion": "my-deployment"})
            .setStringEncoding("utf16")
            .setRange("1")
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "analyzer")
            analyzer.copy({}).write().save(path)
            loaded = ContentUnderstanding.load(path)
            row = loaded.transform(self.documents()).collect()[0]
        self.assertEqual(row.analysis.status, "Succeeded")
        raw = json.loads(row.analysis.rawResponse)
        self.assertIn("future-model-input", raw["usage"]["tokens"])
        submitted = next(request for request in self.requests if request[0] == "POST")
        self.assertIn("stringEncoding=utf16", submitted[1])
        self.assertEqual(
            submitted[2]["modelDeployments"], {"completion": "my-deployment"}
        )
        document = submitted[2]["inputs"][0]
        self.assertEqual(document["range"], "1")
        self.assertEqual(
            base64.b64decode(document["data"]).decode("utf-8"),
            "Synthetic caf\u00e9 document",
        )
        self.assertNotIn("dataBase64", document)

    def test_generated_durable_path_helpers_resume_without_resubmission(self):
        analyzer = self.analyzer()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "journal")
            first = analyzer.writeToPath(
                self.documents(), idCol="documentId", path=path, format="parquet"
            )
            self.assertEqual(first.select("status").first()[0], "Succeeded")
            analyzer.writeToPath(
                self.documents(), idCol="documentId", path=path, format="parquet"
            )
            self.assertEqual(analyzer.readPath(spark, path, "parquet").count(), 1)
        self.assertEqual(
            sum(request[0] == "POST" for request in self.requests),
            1,
        )

    def test_custom_analyzer_dictionary_is_forwarded_only_by_explicit_driver_call(self):
        analyzer = self.analyzer().setAnalyzerId("custom-v1")
        definition = {
            "baseAnalyzerId": "prebuilt-document",
            "config": {"returnDetails": True},
            "fieldSchema": {"fields": {"Supplier": {"type": "string"}}},
        }
        created = json.loads(analyzer.createAnalyzer(definition, allowReplace=False))
        current = json.loads(analyzer.getAnalyzer())
        self.assertEqual(created["status"], "ready")
        self.assertEqual(current["analyzerId"], "custom-v1")
        self.assertEqual(self.requests[0][0], "PUT")
        self.assertIn("allowReplace=false", self.requests[0][1])
        self.assertEqual(self.requests[0][2], definition)

    def test_scalar_bytes_survive_constructor_save_load_copy_and_clear(self):
        payload = bytes([0, 127, 128, 255])
        analyzer = (
            ContentUnderstanding(documentBytes=payload)
            .setEndpoint(f"http://127.0.0.1:{self.server.server_port}")
            .setOutputCol("analysis")
            .setOperationMode("submit")
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "analyzer")
            analyzer.write().save(path)
            loaded = ContentUnderstanding.load(path)
            self.assertEqual(bytes(loaded.getDocumentBytes()), payload)
            replacement = bytes([255, 128, 1, 0])
            copied = loaded.copy({loaded.documentBytes: replacement})
            copied.transform(self.documents()).collect()
            submitted = next(
                request for request in self.requests if request[0] == "POST"
            )
            self.assertEqual(
                base64.b64decode(submitted[2]["inputs"][0]["data"]), replacement
            )
            loaded.clear(loaded.documentBytes)
            loaded.setDocumentBytesCol("body").transform(self.documents()).collect()
            submitted = [request for request in self.requests if request[0] == "POST"][
                -1
            ]
            self.assertEqual(
                base64.b64decode(submitted[2]["inputs"][0]["data"]),
                "Synthetic caf\u00e9 document".encode("utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
