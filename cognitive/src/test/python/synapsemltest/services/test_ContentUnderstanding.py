# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import base64
import json
import os
import pickle
import re
import runpy
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import CodeType
from unittest.mock import patch
from urllib.parse import urlsplit

import mlflow
import mlflow.pyspark.ml
from py4j.protocol import Py4JJavaError
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.contentunderstanding import ContentUnderstanding
from synapse.ml.services.form import AnalyzeDocument
from synapse.ml.services.text import TextSentiment

spark = init_spark()


def reject_media_unpickling():
    raise AssertionError("Raw media bytes must not be unpickled.")


class MediaPickleProbe:
    def __reduce__(self):
        return reject_media_unpickling, ()


class TestContentUnderstanding(unittest.TestCase):
    def setUp(self):
        self.requests = []
        requests = self.requests
        self.request_ports = []
        request_ports = self.request_ports
        self.request_headers = []
        request_headers = self.request_headers

        class Handler(BaseHTTPRequestHandler):
            completed_body = {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "Succeeded",
                "usage": {"tokens": {"future-model-input": 3}},
                "result": {
                    "contents": [
                        {
                            "markdown": "synthetic result",
                            "fields": {
                                "Optional": {"type": "string"},
                                "ExtractedText": {
                                    "type": "string",
                                    "valueString": "synthetic result",
                                    "confidence": 0.94,
                                    "source": "D(1,1,1,2,1,2,2,1,2)",
                                    "spans": [{"offset": 0, "length": 16}],
                                },
                            },
                        },
                        {
                            "fields": {
                                "Flag": {"type": "boolean", "valueBoolean": False}
                            },
                            "metadata": {"futureProperty": [1, True, None]},
                        },
                    ],
                    "futureProperty": {
                        "text": "caf\u00e9",
                        "nested": [{"value": None}],
                    },
                },
            }

            def log_message(self, format, *args):
                pass

            def send_json(self, status, body, location=None, retry_after=None):
                payload = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                if location:
                    self.send_header("Operation-Location", location)
                if retry_after:
                    self.send_header("Retry-After", retry_after)
                self.end_headers()
                self.wfile.write(payload)

            def read_body(self):
                return json.loads(self.rfile.read(int(self.headers["Content-Length"])))

            def do_POST(self):
                requests.append(("POST", self.path, self.read_body()))
                request_ports.append(self.server.server_port)
                request_headers.append(
                    {name.lower(): value for name, value in self.headers.items()}
                )
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
                request_ports.append(self.server.server_port)
                if "/analyzerResults/" in self.path:
                    body = self.completed_body
                else:
                    body = {
                        "analyzerId": urlsplit(self.path).path.split("/")[-1],
                        "status": "ready",
                    }
                self.send_json(200, body)

            def do_PUT(self):
                requests.append(("PUT", self.path, self.read_body()))
                request_ports.append(self.server.server_port)
                self.send_json(
                    201,
                    {
                        "analyzerId": urlsplit(self.path).path.split("/")[-1],
                        "status": "ready",
                    },
                )

        self.Handler = Handler
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self.Handler)
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

    def test_preview_truncation_and_retry_options_survive_copy_save_load(self):
        stage = self.analyzer().setApiVersion("2026-06-01-preview")
        self.assertFalse(stage.getRetryThrottledRequests())
        stage.setRetryThrottledRequests(True).setAllowInputTruncation(False)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "stage")
            stage.write().save(path)
            for candidate in (stage, stage.copy({}), ContentUnderstanding.load(path)):
                self.assertTrue(candidate.getRetryThrottledRequests())
                self.assertIs(candidate.getAllowInputTruncation(), False)
                self.requests.clear()
                candidate.transform(self.documents()).collect()
                post = next(
                    request for request in self.requests if request[0] == "POST"
                )
                self.assertIn("allowInputTruncation=false", post[1])
                self.assertNotIn("allowInputTruncation", json.dumps(post[2]))
                candidate.clear(candidate.allowInputTruncation)
                self.requests.clear()
                candidate.transform(self.documents()).collect()
                post = next(
                    request for request in self.requests if request[0] == "POST"
                )
                self.assertNotIn("allowInputTruncation", post[1])

        self.requests.clear()
        stage.setAllowInputTruncationCol("truncate")
        documents = spark.createDataFrame(
            [
                ("one", bytearray(b"document"), True),
                ("two", bytearray(b"document"), False),
                ("three", bytearray(b"document"), None),
            ],
            "documentId string, body binary, truncate boolean",
        ).coalesce(1)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "column-stage")
            stage.write().save(path)
            for candidate in (stage, stage.copy({}), ContentUnderstanding.load(path)):
                self.assertEqual(candidate.getAllowInputTruncationCol(), "truncate")
                self.assertTrue(candidate.getRetryThrottledRequests())
                self.requests.clear()
                candidate.transform(documents).collect()
                queries = [
                    urlsplit(request[1]).query
                    for request in self.requests
                    if request[0] == "POST"
                ]
                self.assertEqual(
                    queries,
                    [
                        "allowInputTruncation=true&api-version=2026-06-01-preview",
                        "allowInputTruncation=false&api-version=2026-06-01-preview",
                        "api-version=2026-06-01-preview",
                    ],
                )

    def test_pipeline_autologging_omits_credentials_and_preserves_execution(self):
        tracking_uri = mlflow.get_tracking_uri()
        try:
            with tempfile.TemporaryDirectory() as directory:
                mlflow.set_tracking_uri((Path(directory) / "mlruns").as_uri())
                experiment_id = mlflow.create_experiment("service-credentials")
                mlflow.pyspark.ml.autolog(log_models=False)
                for stage_type in (ContentUnderstanding, AnalyzeDocument):
                    stage = (
                        self.analyzer()
                        if stage_type is ContentUnderstanding
                        else AnalyzeDocument()
                        .setUrl(f"http://127.0.0.1:{self.server.server_port}")
                        .setPrebuiltModelId("prebuilt-read")
                        .setImageUrl("https://example.invalid/synthetic-document")
                        .setOutputCol("analysis")
                        .setPollingDelay(0)
                    )
                    stage.setSubscriptionKey("synthetic-key")
                    stage.setAADToken("synthetic-token")
                    stage.setConcurrency(1)
                    path = os.path.join(directory, stage_type.__name__)
                    stage.write().save(path)
                    copied = stage.copy(
                        {
                            stage.subscriptionKey: "synthetic-copy-key",
                            stage.AADToken: "synthetic-copy-token",
                        }
                    )
                    for state, candidate, key, token in (
                        ("configured", stage, "synthetic-key", "synthetic-token"),
                        (
                            "copied",
                            copied,
                            "synthetic-copy-key",
                            "synthetic-copy-token",
                        ),
                        (
                            "loaded",
                            stage_type.load(path),
                            "synthetic-key",
                            "synthetic-token",
                        ),
                    ):
                        with self.subTest(stage=stage_type.__name__, state=state):
                            with mlflow.start_run(experiment_id=experiment_id) as run:
                                model = Pipeline(stages=[candidate]).fit(
                                    self.documents()
                                )
                                self.assertEqual(candidate.getSubscriptionKey(), key)
                                self.assertEqual(candidate.getAADToken(), token)
                                self.request_headers.clear()
                                model.transform(self.documents()).collect()
                                self.assertEqual(len(self.request_headers), 1)
                                self.assertEqual(
                                    self.request_headers[-1][
                                        "ocp-apim-subscription-key"
                                    ],
                                    key,
                                )
                                candidate.clear(candidate.subscriptionKey)
                                self.request_headers.clear()
                                model.transform(self.documents()).collect()
                                self.assertEqual(len(self.request_headers), 1)
                                self.assertEqual(
                                    self.request_headers[-1]["authorization"],
                                    "Bearer " + token,
                                )
                            params = mlflow.get_run(run.info.run_id).data.params
                            self.assertEqual(
                                params[stage_type.__name__ + ".concurrency"], "1"
                            )
                            for name in ("subscriptionKey", "AADToken"):
                                self.assertNotIn(
                                    stage_type.__name__ + "." + name, params
                                )
                            for secret in (key, token):
                                self.assertNotIn(secret, json.dumps(params))
        finally:
            mlflow.pyspark.ml.autolog(disable=True)
            mlflow.set_tracking_uri(tracking_uri)

    def test_job_cancellation_stops_async_polling_and_blocked_responses(self):
        for scenario in ("polling", "blocked", "throttled"):
            for interrupt in (False, True):
                with self.subTest(scenario=scenario, interrupt=interrupt):
                    self.requests.clear()
                    self.Handler.completed_body = {"status": "Running"}
                    started = threading.Event()
                    disconnected = threading.Event()
                    original_get = self.Handler.do_GET
                    requests = self.requests

                    def get(
                        handler,
                        scenario=scenario,
                        started=started,
                        disconnected=disconnected,
                        original_get=original_get,
                        requests=requests,
                    ):
                        if scenario == "throttled":
                            requests.append(("GET", handler.path, None))
                            handler.send_json(429, {}, retry_after="60")
                            started.set()
                        elif scenario == "blocked":
                            requests.append(("GET", handler.path, None))
                            handler.send_response(200)
                            handler.send_header("Content-Length", "1")
                            handler.end_headers()
                            handler.connection.settimeout(10)
                            started.set()
                            try:
                                if handler.rfile.read(1) == b"":
                                    disconnected.set()
                            except (ConnectionResetError, ConnectionAbortedError):
                                disconnected.set()
                            except OSError:
                                pass
                        else:
                            original_get(handler)
                            started.set()

                    self.Handler.do_GET = get
                    stage = (
                        self.analyzer()
                        .setConcurrency(2)
                        .setRetryThrottledRequests(scenario == "throttled")
                        .setPollingDelay(100)
                        .setMaxPollAttempts(300)
                        .setMaxOperationDuration(30000)
                    )
                    documents = self.documents()
                    frame = stage.transform(
                        documents.union(documents).union(documents).coalesce(1)
                    )
                    group = f"cu-cancellation-{scenario}-{interrupt}"
                    errors = []

                    def collect(
                        frame=frame, group=group, interrupt=interrupt, errors=errors
                    ):
                        spark.sparkContext.setJobGroup(
                            group,
                            "CU cancellation regression",
                            interruptOnCancel=interrupt,
                        )
                        try:
                            frame.collect()
                        except Exception as error:
                            errors.append(error)
                        finally:
                            for name in (
                                "spark.jobGroup.id",
                                "spark.job.description",
                                "spark.job.interruptOnCancel",
                            ):
                                spark.sparkContext.setLocalProperty(name, None)

                    worker = threading.Thread(target=collect, daemon=True)
                    worker.start()
                    try:
                        self.assertTrue(started.wait(30), "Polling never started")
                        tracker = spark.sparkContext.statusTracker()
                        stage_ids = {
                            stage_id
                            for job_id in tracker.getJobIdsForGroup(group)
                            for stage_id in tracker.getJobInfo(job_id).stageIds
                        }
                        self.assertTrue(stage_ids)
                        cancellation_deadline = time.monotonic() + 10
                        spark.sparkContext.cancelJobGroup(group)
                        worker.join(
                            timeout=max(0, cancellation_deadline - time.monotonic())
                        )
                        self.assertFalse(worker.is_alive())
                        self.assertTrue(
                            errors, "Cancelled action unexpectedly succeeded"
                        )
                        self.assertIn("cancel", str(errors[0]).lower())
                        while (
                            any(
                                tracker.getStageInfo(stage_id).numActiveTasks
                                for stage_id in stage_ids
                            )
                            and time.monotonic() < cancellation_deadline
                        ):
                            time.sleep(0.05)
                        self.assertTrue(
                            all(
                                tracker.getStageInfo(stage_id).numActiveTasks == 0
                                for stage_id in stage_ids
                            ),
                            "Cancelled executor tasks are still active",
                        )
                        if scenario == "blocked":
                            self.assertTrue(
                                disconnected.wait(5),
                                "In-flight response was not aborted",
                            )
                        time.sleep(0.5)
                        count = len(self.requests)
                        time.sleep(0.75)
                        self.assertEqual(len(self.requests), count)
                    finally:
                        self.Handler.do_GET = original_get
                        self.Handler.completed_body = {"status": "Succeeded"}
                        spark.sparkContext.cancelJobGroup(group)
                        worker.join(timeout=5)
                    row = self.analyzer().setConcurrency(2).transform(documents).first()
                    self.assertEqual(row.analysis.status, "Succeeded")

    def test_generated_wrapper_copy_and_persistence_preserve_request_options(self):
        analyzer = self.analyzer().setParams(
            modelDeployments={"completion": "my-deployment"},
            stringEncoding="utf16",
            range="1",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "analyzer")
            analyzer.copy({}).write().save(path)
            loaded = ContentUnderstanding.load(path)
            self.assertEqual(loaded.getSubscriptionKey(), "synthetic-test-key")
            row = loaded.transform(self.documents()).collect()[0]
        self.assertEqual(row.analysis.status, "Succeeded")
        raw = json.loads(row.analysis.rawResponse)
        self.assertEqual(raw, self.Handler.completed_body)
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

    def test_load_preserves_uid_based_default_columns(self):
        analyzer = (
            ContentUnderstanding()
            .setEndpoint(f"http://127.0.0.1:{self.server.server_port}")
            .setSubscriptionKey("synthetic-test-key")
            .setDocumentBytesCol("body")
            .setDocumentNameCol("documentId")
            .setMimeType("text/plain")
            .setPollingDelay(0)
        )
        output_col = analyzer.getOutputCol()
        error_col = analyzer.getErrorCol()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "default-columns")
            analyzer.write().save(path)
            loaded = ContentUnderstanding.load(path)
            self.assertEqual(loaded.getOutputCol(), output_col)
            self.assertEqual(loaded.getErrorCol(), error_col)
            row = loaded.transform(self.documents()).collect()[0]
        self.assertEqual(row[output_col].status, "Succeeded")
        self.assertIn(error_col, row.asDict())
        self.assertEqual(loaded.getOutputCol(), output_col)

    def guide_blocks(self) -> list[str]:
        repository = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / "build.sbt").is_file()
        )
        guide = (
            repository
            / "docs"
            / "Explore Algorithms"
            / "AI Services"
            / "Content Understanding.md"
        ).read_text(encoding="utf-8")
        return re.findall(r"```python\n(.*?)\n```", guide, re.DOTALL)

    def test_saved_analysis_results_can_be_read_without_resubmission(self):
        blocks = self.guide_blocks()
        examples = {
            "url": next(block for block in blocks if block.startswith("from synapse")),
            "binary": next(block for block in blocks if block.startswith("files = ")),
        }
        projection = next(block for block in blocks if block.startswith("completed = "))
        for source, example in examples.items():
            with self.subTest(
                source=source
            ), tempfile.TemporaryDirectory() as directory:
                self.requests.clear()
                namespace = {
                    "spark": spark,
                    "ContentUnderstanding": ContentUnderstanding,
                    "endpoint": f"http://127.0.0.1:{self.server.server_port}",
                    "key": "synthetic-test-key",
                    "output_path": os.path.join(directory, "results"),
                    "binary_output_path": os.path.join(directory, "binary-results"),
                }
                if source == "url":
                    namespace["documents"] = spark.createDataFrame(
                        [
                            ("https://example.invalid/one.txt",),
                            ("https://example.invalid/two.txt",),
                        ],
                        "documentUrl string",
                    )
                else:
                    input_path = Path(directory) / "input"
                    input_path.mkdir()
                    for name in ("one", "two"):
                        (input_path / (name + ".txt")).write_bytes(
                            b"Synthetic document"
                        )
                    namespace["input_path"] = str(input_path)

                exec(example, namespace)
                self.assertEqual(
                    sum(request[0] == "POST" for request in self.requests), 2
                )
                requests_after_write = list(self.requests)

                exec(projection, namespace)
                markdown = namespace["markdown"]
                self.assertEqual(
                    [row.markdown for row in markdown.collect()],
                    ["synthetic result", "synthetic result"],
                )
                markdown.show(truncate=False)
                self.assertEqual(self.requests, requests_after_write)
                with self.assertRaises(AnalysisException):
                    exec(example, namespace)
                self.assertEqual(self.requests, requests_after_write)

    def test_guide_submit_poll_and_custom_results_are_persisted(self) -> None:
        blocks = self.guide_blocks()
        examples = (
            ("submitted = ", "polled_results", "polledAnalysis"),
            ("custom = ", "custom_results", "analysis"),
        )
        for prefix, result_name, column in examples:
            with self.subTest(
                example=prefix
            ), tempfile.TemporaryDirectory() as directory:
                self.requests.clear()
                example = next(block for block in blocks if block.startswith(prefix))
                namespace = {
                    "spark": spark,
                    "ContentUnderstanding": ContentUnderstanding,
                    "analyzer": self.analyzer().setErrorCol("requestError"),
                    "documents": self.documents(),
                    "endpoint": f"http://127.0.0.1:{self.server.server_port}",
                    "key": "synthetic-test-key",
                    "handles_path": os.path.join(directory, "handles"),
                    "results_path": os.path.join(directory, "results"),
                    "custom_output_path": os.path.join(directory, "custom"),
                }
                exec(example, namespace)
                methods = [request[0] for request in self.requests]
                self.assertEqual(methods.count("POST"), 1)
                self.assertEqual(methods.count("GET"), 1)
                self.assertEqual(methods.count("PUT"), int(prefix == "custom = "))
                requests_after_write = list(self.requests)
                results = namespace[result_name]
                self.assertEqual(results.first()[column].status, "Succeeded")
                results.show(truncate=False)
                self.assertEqual(self.requests, requests_after_write)
                # Creation is eager; repeat only the custom example's persistence portion.
                repeated = (
                    example[example.index("custom.transform(documents)") :]
                    if prefix == "custom = "
                    else example
                )
                with self.assertRaises(AnalysisException):
                    exec(repeated, namespace)
                self.assertEqual(self.requests, requests_after_write)

    def test_guide_retries_only_saved_definite_failures_without_handles(self) -> None:
        example = next(
            block
            for block in self.guide_blocks()
            if block.startswith("saved = spark.read.parquet(output_path)")
        )
        shared = json.dumps({"code": "SharedConfigurationFailure"})
        handle = (
            f"http://127.0.0.1:{self.server.server_port}"
            "/contentunderstanding/analyzerResults/123e4567-e89b-12d3-a456-426614174000"
            "?api-version=2025-11-01"
        )
        cases = (
            ("rejected", None, "Rejected", None),
            ("shared", None, "Failed", shared),
            ("other", None, "Failed", json.dumps({"code": "OtherFailure"})),
            ("unknown", None, "Unknown", None),
            ("running", handle, "Running", None),
            ("accepted-rejection", handle, "Rejected", None),
            ("accepted-failure", handle, "Failed", shared),
            ("completed", handle, "Succeeded", None),
        )
        saved = spark.createDataFrame(
            [
                (name, bytearray(b"document"), (location, status, error), None)
                for name, location, status, error in cases
            ],
            "documentId string, body binary, "
            "analysis struct<operationLocation:string,status:string,error:string>, "
            "requestError string",
        )
        with tempfile.TemporaryDirectory() as directory:
            namespace = {
                "spark": spark,
                "analyzer": self.analyzer().setErrorCol("requestError"),
                "output_path": os.path.join(directory, "saved"),
                "retry_output_path": os.path.join(directory, "retried"),
            }
            saved.write.parquet(namespace["output_path"])
            self.assertEqual(self.requests, [])
            exec(example, namespace)
            methods = [request[0] for request in self.requests]
            self.assertEqual(methods.count("POST"), 2)
            self.assertEqual(methods.count("GET"), 2)
            self.assertNotIn("PUT", methods)
            requests_after_write = list(self.requests)
            results = spark.read.parquet(namespace["retry_output_path"])
            self.assertEqual(
                sorted(row.documentId for row in results.collect()),
                ["rejected", "shared"],
            )
            results.show(truncate=False)
            self.assertEqual(self.requests, requests_after_write)
            with self.assertRaises(AnalysisException):
                exec(example, namespace)
            self.assertEqual(self.requests, requests_after_write)

    def notebook_cells(self) -> dict[str, CodeType]:
        """Cell IDs and recovery globals are the notebook's offline test contract."""
        repository = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / "build.sbt").is_file()
        )
        notebook_path = (
            repository
            / "docs"
            / "Explore Algorithms"
            / "AI Services"
            / "Quickstart - Semantic Extraction with Content Understanding.ipynb"
        )
        notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
        cells = {
            cell["id"]: compile(
                "".join(cell["source"]), f"{notebook_path}#{cell['id']}", "exec"
            )
            for cell in notebook["cells"]
            if cell["cell_type"] == "code"
        }
        self.assertEqual(
            list(cells),
            [
                "configuration",
                "sample-input",
                "create-analyzer",
                "submit-and-save",
                "recovery-paths",
                "poll-and-save",
                "display-fields",
            ],
            "The notebook cell contract changed; update the actual-cell recovery test.",
        )
        return cells

    def test_semantic_notebook_rejects_invalid_configuration_before_service_calls(self):
        cells = self.notebook_cells()
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "CONTENT_UNDERSTANDING_ENDPOINT": (
                    f"http://127.0.0.1:{self.server.server_port}"
                ),
                "CONTENT_UNDERSTANDING_API_KEY": "synthetic-test-key",
                "CONTENT_UNDERSTANDING_OUTPUT_PATH": directory,
                "CONTENT_UNDERSTANDING_RUN_ID": "configuration-check",
                "CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS": "{}",
            },
        ):
            for endpoint in (
                "https://user@example.invalid",
                "ftp://example.invalid",
                "https:///missing-host",
                "https://example.invalid:bad",
                "https://example.invalid:65536",
            ):
                with self.subTest(endpoint=endpoint), patch.dict(
                    os.environ, {"CONTENT_UNDERSTANDING_ENDPOINT": endpoint}
                ):
                    with self.assertRaises(ValueError):
                        exec(cells["configuration"], {})
            configured = {}
            exec(cells["configuration"], configured)
            origin = configured["resource_origin"]
            for implicit, explicit in (
                ("https://EXAMPLE.invalid", "https://example.invalid:443/operation"),
                ("http://example.invalid", "http://example.invalid:80/operation"),
            ):
                with self.subTest(origin=implicit):
                    self.assertEqual(origin(implicit), origin(explicit))
            for run_id in ("../escape", "a/b", "a" * 40, "-bad"):
                with self.subTest(run_id=run_id), patch.dict(
                    os.environ, {"CONTENT_UNDERSTANDING_RUN_ID": run_id}
                ):
                    with self.assertRaisesRegex(ValueError, "Run ID must be"):
                        exec(cells["configuration"], {})
            for mapping in (
                "[1]",
                "null",
                "true",
                '"name"',
                '{"m": 1}',
                '{"": "x"}',
                "{",
            ):
                with self.subTest(mapping=mapping), patch.dict(
                    os.environ, {"CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS": mapping}
                ):
                    configured = {}
                    exec(cells["configuration"], configured)
                    with self.assertRaisesRegex(ValueError, "Model deployments must"):
                        exec(cells["create-analyzer"], configured)
            self.assertEqual(self.requests, [])
            for mapping in ("", "   ", "{}", '{"model": "deployment"}'):
                with self.subTest(mapping=mapping), patch.dict(
                    os.environ, {"CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS": mapping}
                ):
                    configured = {}
                    exec(cells["configuration"], configured)
                    exec(cells["create-analyzer"], configured)
            self.assertEqual([request[0] for request in self.requests], ["PUT"] * 4)
            configured = {}
            exec(cells["configuration"], configured)
            with patch.object(
                ContentUnderstanding,
                "createAnalyzer",
                return_value='{"status":"creating"}',
            ), self.assertRaisesRegex(RuntimeError, "not ready"):
                exec(cells["create-analyzer"], configured)
            self.assertEqual([request[0] for request in self.requests], ["PUT"] * 4)

    def test_semantic_notebook_executes_and_recovers_saved_handles(self):
        self.assert_semantic_notebook_run({})

    def test_semantic_notebook_sends_deployments_only_with_analysis(self):
        self.assert_semantic_notebook_run({"model": "deployment"})

    def test_semantic_notebook_ci_runner_recovers_without_resubmitting(self):
        self.assert_semantic_notebook_run({}, ci_runner=True)

    def test_semantic_notebook_ci_runner_retains_analyzer_service_error(self):
        key = 'synthetic-"secret\\value'
        service_error = {
            "code": "InvalidRequest",
            "message": f"Synthetic analyzer rejection: {key}",
            "innererror": {"code": "SyntheticConfigurationError"},
            "details": [{"code": "InvalidValue", "target": "config"}],
        }

        def put(handler):
            self.requests.append(("PUT", handler.path, handler.read_body()))
            handler.send_json(400, {"error": service_error})

        self.Handler.do_PUT = put
        cells = self.notebook_cells()
        notebook = Path(cells["configuration"].co_filename.rsplit("#", 1)[0])
        repository = next(
            parent for parent in notebook.parents if (parent / "build.sbt").is_file()
        )
        runner = runpy.run_path(
            str(repository / "tools" / "ci" / "run_content_understanding_notebook.py")
        )
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "CONTENT_UNDERSTANDING_ENDPOINT": (
                    f"http://127.0.0.1:{self.server.server_port}"
                ),
                "CONTENT_UNDERSTANDING_API_KEY": key,
                "CONTENT_UNDERSTANDING_OUTPUT_PATH": directory,
                "CONTENT_UNDERSTANDING_RUN_ID": "loopback-rejected-analyzer",
                "CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS": "",
            },
        ):
            report = {}
            with self.assertRaises(Py4JJavaError) as raised:
                runner["execute_notebook"](notebook, report)
            self.assertEqual(report["phase"], "create-analyzer")
            diagnostic = runner["service_error"](raised.exception)
            self.assertEqual(diagnostic, {"httpStatus": 400, "error": service_error})
            redacted = json.loads(runner["redact"](json.dumps(diagnostic), key))
            self.assertEqual(
                redacted["error"]["message"],
                "Synthetic analyzer rejection: [REDACTED]",
            )
            self.assertEqual([method for method, _, _ in self.requests], ["PUT"])

    def assert_semantic_notebook_run(
        self, deployments: dict[str, str], ci_runner: bool = False
    ) -> None:
        cells = self.notebook_cells()
        supplier = "Contoso Office Supplies"
        prefix = "\U0001f4e6 "
        source = "D(1,1,1,2,1,2,2,1,2)"
        self.Handler.completed_body["result"]["contents"] = [
            {
                "markdown": prefix + supplier + "\nTotal: 125.00 USD",
                "fields": {
                    "Supplier": {
                        "type": "string",
                        "valueString": supplier,
                        "confidence": 0.94,
                        "source": source,
                        "spans": [{"offset": len(prefix), "length": len(supplier)}],
                    },
                    "TotalAmount": {"type": "number", "valueNumber": 125.0},
                    "RequestedDeliveryDate": {
                        "type": "date",
                        "valueDate": "2026-02-01",
                    },
                    "OrderSummary": {
                        "type": "string",
                        "valueString": "A synthetic office-supply purchase.",
                    },
                },
            },
            {"markdown": "", "fields": {"Optional": {"type": "string"}}},
        ]
        original_get = self.Handler.do_GET
        poll_count = 0

        def get(handler):
            nonlocal poll_count
            if "/analyzerResults/" in handler.path:
                poll_count += 1
                if poll_count == 1:
                    self.requests.append(("GET", handler.path, None))
                    self.request_ports.append(handler.server.server_port)
                    handler.send_json(200, {"status": "Running"})
                    return
            original_get(handler)

        self.Handler.do_GET = get
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "CONTENT_UNDERSTANDING_ENDPOINT": (
                    f"http://127.0.0.1:{self.server.server_port}"
                ),
                "CONTENT_UNDERSTANDING_API_KEY": "synthetic-test-key",
                "CONTENT_UNDERSTANDING_OUTPUT_PATH": directory,
                "CONTENT_UNDERSTANDING_RUN_ID": "loopback-notebook",
                "CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS": json.dumps(deployments),
            },
        ):
            if ci_runner:
                notebook = Path(cells["configuration"].co_filename.rsplit("#", 1)[0])
                repository = next(
                    parent
                    for parent in notebook.parents
                    if (parent / "build.sbt").is_file()
                )
                runner = runpy.run_path(
                    str(
                        repository
                        / "tools"
                        / "ci"
                        / "run_content_understanding_notebook.py"
                    )
                )
                runtime = runner["runtime_evidence"]
                with patch.dict(
                    os.environ,
                    {
                        "BUILD_SOURCEVERSION": "loopback-build",
                        "BUILD_SOURCEBRANCH": "refs/heads/loopback",
                    },
                ), patch.object(
                    runner["subprocess"],
                    "check_output",
                    return_value="loopback-build\n",
                ), patch.dict(
                    runtime.__globals__,
                    {
                        "verify_wheel": lambda *args: {"version": "loopback"},
                        "verify_jar": lambda root, module, version, location: {
                            "location": location
                        },
                    },
                ):
                    evidence = runtime(repository, spark)
                self.assertEqual(evidence["spark"], spark.version)
                self.assertTrue(evidence["scala"])
                self.assertTrue(evidence["java"])
                report = {}
                runner["execute_notebook"](notebook, report)
                self.assertEqual(
                    report["recovery"],
                    {"sameOperation": True, "sameFields": True, "status": "Succeeded"},
                )
                methods = [method for method, _, _ in self.requests]
                self.assertEqual(methods.count("POST"), 1)
                self.assertEqual(methods.count("PUT"), 1)
                self.assertEqual(poll_count, 3)
                supplier_field = next(
                    field
                    for field in report["fields"]
                    if field["fieldName"] == "Supplier"
                )
                self.assertEqual(supplier_field["supportingText"], [supplier])
                self.assertEqual(supplier_field["confidence"], 0.94)
                return
            initial = {}
            for name, code in cells.items():
                exec(code, initial)
                if name == "configuration":
                    self.assertTrue(
                        {
                            "max_poll_attempts",
                            "polling_delay_ms",
                            "run_root",
                            "endpoint_origin",
                        }.issubset(initial),
                        "Notebook configuration globals used by recovery changed.",
                    )
                    initial["max_poll_attempts"] = 1
                    initial["polling_delay_ms"] = 0
            self.assertEqual(initial["results"].first().analysis.status, "Running")
            self.assertEqual(poll_count, 1)

            requests_before_rerun = list(self.requests)
            for name in ("submit-and-save", "poll-and-save"):
                with self.assertRaises(AnalysisException):
                    exec(cells[name], initial)
                self.assertEqual(self.requests, requests_before_rerun)
                self.assertEqual(poll_count, 1)

            os.environ["CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS"] = "invalid-but-unused"
            requests_before_recovery = list(self.requests)
            with patch.dict(
                os.environ,
                {
                    "CONTENT_UNDERSTANDING_ENDPOINT": "http://127.0.0.1:1",
                },
            ):
                wrong_resource = {}
                exec(cells["configuration"], wrong_resource)
                wrong_resource["recovery_input_path"] = initial["recovery_output_path"]
                wrong_resource["recovery_output_path"] = (
                    wrong_resource["run_root"] + "/wrong-resource"
                )
                with self.assertRaisesRegex(ValueError, "do not match"):
                    exec(cells["poll-and-save"], wrong_resource)
                self.assertFalse(Path(wrong_resource["recovery_output_path"]).exists())
                self.assertEqual(self.requests, requests_before_recovery)

            resumed = {}
            exec(cells["configuration"], resumed)
            resumed["max_poll_attempts"] = 1
            resumed["polling_delay_ms"] = 0
            resumed["recovery_input_path"] = initial["recovery_output_path"]
            resumed["recovery_output_path"] = resumed["run_root"] + "/poll-002"
            exec(cells["poll-and-save"], resumed)
            exec(cells["display-fields"], resumed)
            self.assertEqual(resumed["results"].first().analysis.status, "Succeeded")
            self.assertEqual(poll_count, 2)
            requests_after_poll = list(self.requests)
            recovery_requests = requests_after_poll[len(requests_before_recovery) :]
            self.assertTrue(recovery_requests)
            self.assertTrue(
                all(
                    method == "GET" and "/analyzerResults/" in path
                    for method, path, _ in recovery_requests
                )
            )
            fields = {row.fieldName: row for row in resumed["extracted"].collect()}
            self.assertEqual(fields["Supplier"].value, supplier)
            self.assertEqual(fields["Supplier"].confidence, 0.94)
            self.assertEqual(fields["Supplier"].source, source)
            self.assertEqual(fields["Supplier"].supportingText, [supplier])
            self.assertEqual(fields["TotalAmount"].value, "125.0")
            self.assertEqual(fields["RequestedDeliveryDate"].value, "2026-02-01")
            self.assertIsNone(fields["OrderSummary"].confidence)
            self.assertIsNone(fields["OrderSummary"].source)
            self.assertIsNone(fields["OrderSummary"].supportingText)
            self.assertIsNone(fields["Optional"].value)
            self.assertEqual(fields["Optional"].contentIndex, 1)

            resumed["recovery_input_path"] = resumed["recovery_output_path"]
            resumed["recovery_output_path"] = resumed["run_root"] + "/poll-003"
            exec(cells["poll-and-save"], resumed)
            self.assertEqual(resumed["results"].first().analysis.status, "Succeeded")
            self.assertEqual(self.requests, requests_after_poll)

        submissions = [request for request in self.requests if request[0] == "POST"]
        self.assertEqual(len(submissions), 1)
        self.assertIn("stringEncoding=codePoint", submissions[0][1])
        if deployments:
            self.assertEqual(submissions[0][2]["modelDeployments"], deployments)
        else:
            self.assertNotIn("modelDeployments", submissions[0][2])
        document = submissions[0][2]["inputs"][0]
        self.assertTrue(base64.b64decode(document["data"]).startswith(b"%PDF-1.4"))
        self.assertEqual(document["mimeType"], "application/pdf")
        creations = [request[2] for request in self.requests if request[0] == "PUT"]
        self.assertEqual(len(creations), 1)
        creation = creations[0]
        self.assertNotIn("modelDeployments", creation)
        self.assertEqual(creation["baseAnalyzerId"], "prebuilt-document")
        self.assertTrue(creation["config"]["estimateFieldSourceAndConfidence"])
        self.assertEqual(
            creation["fieldSchema"]["fields"]["OrderSummary"]["method"], "generate"
        )

    def test_saved_submit_handles_can_be_polled_without_resubmission(self):
        analyzer = self.analyzer().setOperationMode("submit")
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "handles")
            analyzer.transform(self.documents()).write.parquet(path)
            handles = spark.read.parquet(path).selectExpr(
                "analysis.operationLocation AS handle"
            )
            poller = (
                analyzer.copy({})
                .setOperationMode("poll")
                .setOperationLocationCol("handle")
            )
            self.assertEqual(
                poller.transform(handles).first().analysis.status, "Succeeded"
            )
        self.assertEqual(
            sum(request[0] == "POST" for request in self.requests),
            1,
        )

    def test_loaded_column_defaults_survive_clear_copy_execution_and_resave(self):
        documents = self.documents()
        names = ("outputCol", "errorCol")
        for configured in ((), ("outputCol",), ("errorCol",), names):
            with self.subTest(configured=configured):
                analyzer = self.analyzer()
                analyzer.clear(analyzer.outputCol)
                defaults = {name: analyzer.getOrDefault(name) for name in names}
                explicit = {name: "custom_" + name for name in configured}
                analyzer.setParams(**explicit)
                with tempfile.TemporaryDirectory() as directory:
                    path = os.path.join(directory, "original")
                    analyzer.save(path)
                    loaded = ContentUnderstanding.load(path)
                    for candidate in (loaded, loaded.copy({})):
                        self.assertEqual(candidate.uid, analyzer.uid)
                        for name in names:
                            self.assertEqual(
                                candidate.getOrDefault(name),
                                explicit.get(name, defaults[name]),
                            )
                            candidate.clear(candidate.getParam(name))
                            self.assertEqual(
                                candidate.getOrDefault(name), defaults[name]
                            )
                        row = candidate.transform(documents).first()
                        self.assertEqual(
                            set(row.asDict()),
                            set(documents.columns) | set(defaults.values()),
                        )
                        self.assertEqual(row[defaults["outputCol"]].status, "Succeeded")
                        self.assertIsNone(row[defaults["errorCol"]])
                    resaved = os.path.join(directory, "cleared")
                    loaded.save(resaved)
                    reloaded = ContentUnderstanding.load(resaved)
                    for name in names:
                        reloaded.clear(reloaded.getParam(name))
                        self.assertEqual(reloaded.getOrDefault(name), defaults[name])
                    row = reloaded.transform(documents).first()
                    self.assertEqual(row[defaults["outputCol"]].status, "Succeeded")
                    self.assertIsNone(row[defaults["errorCol"]])

    def test_endpoint_replacement_survives_copy_and_load(self):
        replacement = ThreadingHTTPServer(("127.0.0.1", 0), self.Handler)
        thread = threading.Thread(target=replacement.serve_forever, daemon=True)
        thread.start()
        endpoint = f"http://127.0.0.1:{replacement.server_port}"
        try:
            analyzer = self.analyzer().setOperationMode("submit")
            copied = analyzer.copy({}).setEndpoint(endpoint)
            copied.transform(self.documents()).collect()
            with tempfile.TemporaryDirectory() as directory:
                path = os.path.join(directory, "analyzer")
                analyzer.write().save(path)
                loaded = ContentUnderstanding.load(path).setEndpoint(endpoint)
                loaded.transform(self.documents()).collect()
            self.assertEqual(self.request_ports, [replacement.server_port] * 2)
        finally:
            replacement.shutdown()
            replacement.server_close()
            thread.join(timeout=5)

    def test_java_object_wrapping_preserves_identity_settings_and_original_defaults(
        self,
    ):
        documents = self.documents()
        names = ("outputCol", "errorCol")
        for configured in ((), ("outputCol",), ("errorCol",), names):
            with self.subTest(configured=configured):
                original = self.analyzer().setConcurrency(2)
                original.clear(original.outputCol)
                defaults = {name: original.getOrDefault(name) for name in names}
                explicit = {name: "native_" + name for name in configured}
                original.setParams(**explicit)
                original._transfer_params_to_java()
                wrapped = ContentUnderstanding(java_obj=original._java_obj)
                self.assertEqual(wrapped.uid, original.uid)
                self.assertEqual(wrapped.getUrl(), original.getUrl())
                self.assertEqual(wrapped.getConcurrency(), 2)
                self.assertEqual(wrapped.getPollingDelay(), 0)
                self.assertFalse(wrapped.isDefined(wrapped.documentBytes))
                with tempfile.TemporaryDirectory() as directory:
                    path = os.path.join(directory, "wrapped")
                    wrapped.save(path)
                    for name in names:
                        java_param = original._java_obj.getParam(name)
                        self.assertEqual(
                            original._java_obj.getDefault(java_param).get(),
                            defaults[name],
                        )
                    loaded = ContentUnderstanding.load(path)
                    for candidate in (wrapped, wrapped.copy({}), loaded):
                        self.assertEqual(candidate.uid, original.uid)
                        for name in names:
                            self.assertEqual(
                                candidate.getOrDefault(name),
                                explicit.get(name, defaults[name]),
                            )
                            candidate.clear(candidate.getParam(name))
                            self.assertEqual(
                                candidate.getOrDefault(name), defaults[name]
                            )
                        row = candidate.transform(documents).first()
                        self.assertEqual(
                            set(row.asDict()),
                            set(documents.columns) | set(defaults.values()),
                        )
                        self.assertEqual(row[defaults["outputCol"]].status, "Succeeded")
                        self.assertIsNone(row[defaults["errorCol"]])

    def test_custom_service_name_survives_python_parameter_transfer(self):
        expected = (
            "https://contoso.cognitiveservices.azure.com/contentunderstanding/analyzers"
        )
        analyzer = self.analyzer().setCustomServiceName("contoso")
        analyzer._transfer_params_to_java()
        self.assertEqual(analyzer._java_obj.getUrl(), expected)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "analyzer")
            self.analyzer().write().save(path)
            loaded = ContentUnderstanding.load(path).setCustomServiceName("contoso")
            loaded._transfer_params_to_java()
            self.assertEqual(loaded._java_obj.getUrl(), expected)

    def test_document_bytes_rejects_text_values(self):
        for value in ("body", 10, True):
            with self.assertRaisesRegex(TypeError, "setDocumentBytesCol"):
                ContentUnderstanding().setDocumentBytes(value)

    def test_set_params_rejects_conflicting_bindings_without_mutation(self):
        bindings = [("documentBytes", b"new-document"), ("documentBytesCol", "newBody")]
        for arguments in (bindings, list(reversed(bindings))):
            with self.subTest(arguments=arguments):
                analyzer = self.analyzer()
                analyzer.set(analyzer.documentBytes, bytearray(b"pending-document"))
                pending = dict(analyzer._paramMap)
                with self.assertRaisesRegex(ValueError, "Cannot set both"):
                    analyzer.setParams(outputCol="changed", **dict(arguments))
                self.assertEqual(analyzer._paramMap, pending)
                self.assertEqual(analyzer._java_obj.getDocumentBytesCol(), "body")
                self.assertEqual(analyzer.getOutputCol(), "analysis")

    def test_set_params_rejects_service_none_without_mutation(self):
        for argument in ("documentBytes", "documentBytesCol", "subscriptionKey"):
            with self.subTest(argument=argument):
                analyzer = self.analyzer().setDocumentBytes(b"old-document")
                analyzer.set(analyzer.documentBytes, bytearray(b"pending-document"))
                analyzer.set(analyzer.subscriptionKey, "pending-key")
                pending = dict(analyzer._paramMap)
                with self.assertRaisesRegex(
                    TypeError, f"Service parameter '{argument}' cannot be None"
                ):
                    analyzer.setParams(documentName="changed", **{argument: None})
                self.assertEqual(analyzer._paramMap, pending)
                self.assertEqual(
                    bytes(analyzer._java_obj.getDocumentBytes()), b"old-document"
                )
                self.assertEqual(
                    analyzer._java_obj.getSubscriptionKey(), "synthetic-test-key"
                )
                self.assertEqual(analyzer._java_obj.getDocumentNameCol(), "documentId")

    def test_set_params_preserves_ordinary_none_as_noop(self):
        analyzer = self.analyzer().setPollingDelay(17).setMaxPollAttempts(3)
        pending = dict(analyzer._paramMap)
        self.assertIs(
            analyzer.setParams(pollingDelay=None, maxPollAttempts=None, outputCol=None),
            analyzer,
        )
        self.assertEqual(analyzer._paramMap, pending)
        analyzer._transfer_params_to_java()
        self.assertEqual(analyzer._java_obj.getPollingDelay(), 17)
        self.assertEqual(analyzer._java_obj.getMaxPollAttempts(), 3)
        self.assertEqual(analyzer._java_obj.getOutputCol(), "analysis")

    def test_explicit_service_setters_replace_pending_python_values(self):
        analyzer = self.analyzer()
        analyzer.set(analyzer.subscriptionKey, "stale-key")
        analyzer.setSubscriptionKey("current-key")
        self.assertFalse(analyzer.isSet(analyzer.subscriptionKey))
        analyzer.set(analyzer.documentBytes, bytearray([1]))
        analyzer.setDocumentBytesCol("body")
        analyzer._transfer_params_to_java()
        self.assertEqual(analyzer._java_obj.getSubscriptionKey(), "current-key")
        self.assertEqual(analyzer._java_obj.getDocumentBytesCol(), "body")
        copied = analyzer.copy({})
        self.assertFalse(copied.isSet(copied.subscriptionKey))
        self.assertEqual(copied.getSubscriptionKey(), "current-key")

    def test_invalid_pending_service_values_block_retries_until_corrected(self):
        for column_bound in (False, True):
            for invalid in ("not-document-bytes", None):
                with self.subTest(column_bound=column_bound, invalid=invalid):
                    self.requests.clear()
                    analyzer = self.analyzer()
                    if not column_bound:
                        analyzer.setDocumentBytes(b"old-document")
                    analyzer.set(analyzer.documentBytes, invalid)
                    for _ in range(2):
                        with self.assertRaises(TypeError):
                            analyzer.transform(self.documents()).collect()
                    with self.assertRaises(TypeError):
                        analyzer.copy({})
                    with tempfile.TemporaryDirectory() as directory:
                        with self.assertRaises(TypeError):
                            analyzer.write().save(os.path.join(directory, "stage"))
                    self.assertIn(analyzer.documentBytes, analyzer._paramMap)
                    self.assertEqual(
                        analyzer._paramMap[analyzer.documentBytes], invalid
                    )
                    self.assertEqual(self.requests, [])
                    analyzer.setDocumentBytes(b"corrected-document")
                    row = analyzer.transform(self.documents()).first()
                    self.assertEqual(row.analysis.status, "Succeeded")
                    submitted = next(
                        request for request in self.requests if request[0] == "POST"
                    )
                    self.assertEqual(
                        base64.b64decode(submitted[2]["inputs"][0]["data"]),
                        b"corrected-document",
                    )

    def test_generic_inspection_does_not_read_jvm_service_scalars(self):
        for stage in (ContentUnderstanding(), AnalyzeDocument()):
            with self.subTest(stage=type(stage).__name__):
                stage.setSubscriptionKey("synthetic-key")
                stage.setAADToken("synthetic-token")
                with patch.object(
                    stage, "_java_service_value", wraps=stage._java_service_value
                ) as lookup:
                    values = stage.extractParamMap()
                    for param in (stage.subscriptionKey, stage.AADToken):
                        self.assertFalse(stage.isSet(param))
                        self.assertFalse(stage.hasDefault(param))
                        self.assertFalse(stage.isDefined(param))
                        self.assertNotIn(param, values)
                        with self.assertRaises(KeyError):
                            stage.getOrDefault(param)
                    lookup.assert_not_called()

    def test_pending_service_scalar_does_not_import_jvm_default(self):
        for stage in (ContentUnderstanding(), AnalyzeDocument()):
            with self.subTest(stage=type(stage).__name__):
                stage.set(stage.apiVersion, "pending-version")
                with patch.object(
                    stage, "_java_service_value", wraps=stage._java_service_value
                ) as lookup:
                    self.assertTrue(stage.isSet(stage.apiVersion))
                    self.assertFalse(stage.hasDefault(stage.apiVersion))
                    self.assertEqual(
                        stage.getOrDefault(stage.apiVersion), "pending-version"
                    )
                    lookup.assert_not_called()

    def test_custom_headers_column_setter_and_getter_reach_the_jvm(self):
        analyzer = self.analyzer().setCustomHeadersCol("headers")
        analyzer._transfer_params_to_java()
        self.assertEqual(analyzer._java_obj.getCustomHeadersCol(), "headers")

    def test_existing_service_wrapper_preserves_jvm_and_pending_python_parameters(self):
        service = AnalyzeDocument().setSubscriptionKey("synthetic-key")
        service._transfer_params_to_java()
        self.assertFalse(service.isSet(service.subscriptionKey))
        copied = service.copy({})
        self.assertEqual(copied.getSubscriptionKey(), "synthetic-key")
        pending = AnalyzeDocument()
        pending.set(pending.subscriptionKey, "pending-key")
        self.assertTrue(pending.isSet(pending.subscriptionKey))
        self.assertEqual(pending.getOrDefault(pending.subscriptionKey), "pending-key")
        self.assertEqual(pending.getSubscriptionKey(), "pending-key")
        self.assertEqual(
            pending.extractParamMap()[pending.subscriptionKey], "pending-key"
        )
        self.assertEqual(pending.copy({}).getSubscriptionKey(), "pending-key")
        pending._transfer_params_to_java()
        self.assertEqual(pending.getSubscriptionKey(), "pending-key")
        self.assertFalse(pending.isDefined(pending.subscriptionKey))
        with self.assertRaises(KeyError):
            pending.getOrDefault(pending.subscriptionKey)
        self.assertNotIn(pending.subscriptionKey, pending.extractParamMap())
        self.assertEqual(
            pending.extractParamMap({pending.subscriptionKey: "export-override"})[
                pending.subscriptionKey
            ],
            "export-override",
        )
        overridden = service.copy({service.subscriptionKey: "replacement-key"})
        self.assertEqual(overridden.getSubscriptionKey(), "replacement-key")
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "service")
            service.write().save(path)
            self.assertEqual(
                AnalyzeDocument.load(path).getSubscriptionKey(), "synthetic-key"
            )
        for stage in (service, AnalyzeDocument().setParams(subscriptionKey="pending")):
            stage.clear(stage.subscriptionKey)
            self.assertFalse(stage.isSet(stage.subscriptionKey))
            self.assertNotIn(stage.subscriptionKey, stage._paramMap)
            self.assertFalse(
                stage._java_obj.isSet(stage._java_obj.getParam("subscriptionKey"))
            )

    def test_generated_service_getters_prefer_pending_scalars_without_transferring(
        self,
    ):
        for stage in (ContentUnderstanding(), AnalyzeDocument()):
            with self.subTest(stage=type(stage).__name__):
                stage.setSubscriptionKey("old-key")
                stage.set(stage.subscriptionKey, "pending-key")
                self.assertEqual(stage.getSubscriptionKey(), "pending-key")
                self.assertEqual(
                    stage.getOrDefault(stage.subscriptionKey), "pending-key"
                )
                self.assertEqual(stage._java_obj.getSubscriptionKey(), "old-key")
                stage.setCustomHeaders({"X-Test": "old"})
                stage.set(stage.customHeaders, {"X-Test": "pending"})
                self.assertEqual(stage.getCustomHeaders(), {"X-Test": "pending"})
                self.assertEqual(
                    stage.getOrDefault(stage.customHeaders), {"X-Test": "pending"}
                )

    def test_telemetry_header_descriptions_redact_scalar_values(self):
        for stage in (ContentUnderstanding(), AnalyzeDocument()):
            param = stage.getParam("telemHeaders")
            for pending in (False, True):
                with self.subTest(stage=type(stage).__name__, pending=pending):
                    value = {"X-Test": "synthetic-telemetry"}
                    if pending:
                        stage.set(param, value)
                    else:
                        stage.setTelemHeaders(value)
                    self.assertNotIn("synthetic-telemetry", stage.explainParam(param))
                    self.assertNotIn("synthetic-telemetry", stage.explainParams())
                    self.assertIn("[redacted]", stage.explainParam(param))
                    self.assertEqual(stage.getTelemHeaders(), value)
                    if pending:
                        self.assertEqual(stage.getOrDefault(param), value)
                    else:
                        with self.assertRaises(KeyError):
                            stage.getOrDefault(param)
            stage._transfer_params_to_java()
            stage._java_obj.setVectorParam(
                stage._java_obj.getParam("telemHeaders"), "telemetry-input"
            )
            self.assertIn("telemetry-input", stage.explainParam(param))
            self.assertNotIn(param, stage.extractParamMap())

    def test_default_columns_are_unique_and_allow_pipeline_composition(self):
        first = (
            ContentUnderstanding()
            .setEndpoint(f"http://127.0.0.1:{self.server.server_port}")
            .setDocumentBytesCol("body")
        )
        second = (
            ContentUnderstanding()
            .setEndpoint(f"http://127.0.0.1:{self.server.server_port}")
            .setDocumentBytesCol("body")
        )
        self.assertNotEqual(first.getOutputCol(), second.getOutputCol())
        self.assertNotEqual(first.getErrorCol(), second.getErrorCol())
        schema = (
            Pipeline(stages=[first, second])
            .fit(self.documents())
            .transform(self.documents())
            .schema
        )
        self.assertIn(first.getOutputCol(), schema.fieldNames())
        self.assertIn(second.getOutputCol(), schema.fieldNames())
        self.assertIn(first.getErrorCol(), schema.fieldNames())
        self.assertIn(second.getErrorCol(), schema.fieldNames())

    def test_jvm_service_defaults_survive_copy_load_and_clear(self):
        for stage_type in (ContentUnderstanding, AnalyzeDocument):
            with self.subTest(stage=stage_type.__name__):
                service = stage_type()
                names = ["apiVersion"]
                if stage_type is ContentUnderstanding:
                    names.append("analyzerId")
                defaults = {
                    name: getattr(service, "get" + name[0].upper() + name[1:])()
                    for name in names
                }
                for name in names:
                    self.assertFalse(service.isSet(name))
                with tempfile.TemporaryDirectory() as directory:
                    service.copy({}).write().save(directory + "/stage")
                    for candidate in (service, stage_type.load(directory + "/stage")):
                        for name, value in defaults.items():
                            param = candidate.getParam(name)
                            suffix = name[0].upper() + name[1:]
                            self.assertFalse(candidate.hasDefault(param))
                            self.assertFalse(candidate.isDefined(param))
                            self.assertEqual(
                                getattr(candidate, "get" + suffix)(), value
                            )
                            self.assertNotIn(param, candidate.extractParamMap())
                            setter = "set" + suffix
                            getattr(candidate, setter)("custom-value")
                            self.assertEqual(
                                getattr(candidate, "get" + suffix)(), "custom-value"
                            )
                            getattr(candidate, setter + "Col")("input")
                            copied = candidate.copy({})
                            self.assertEqual(
                                getattr(copied, "get" + suffix + "Col")(), "input"
                            )
                            self.assertFalse(copied.hasDefault(param))
                            self.assertFalse(copied.isDefined(param))
                            with self.assertRaises(KeyError):
                                copied.getOrDefault(param)
                            copied.clear(param)
                            self.assertFalse(copied.isSet(param))
                            self.assertFalse(copied.isDefined(param))
                            self.assertEqual(getattr(copied, "get" + suffix)(), value)
                            candidate.clear(param)
                            candidate._transfer_params_to_java()
                            self.assertEqual(
                                getattr(candidate, "get" + suffix)(), value
                            )

    def test_map_service_parameters_round_trip_as_python_dictionaries(self):
        for stage_type in (ContentUnderstanding, AnalyzeDocument):
            names = ["customHeaders", "telemHeaders"]
            if stage_type is ContentUnderstanding:
                names.append("modelDeployments")
            for name in names:
                for expected in ({}, {"option": "synthetic-value"}):
                    with self.subTest(
                        stage=stage_type.__name__, param=name, value=expected
                    ):
                        service = stage_type()
                        suffix = name[0].upper() + name[1:]
                        getattr(service, "set" + suffix)(expected)
                        with tempfile.TemporaryDirectory() as directory:
                            path = os.path.join(directory, "stage")
                            service.write().save(path)
                            candidates = (
                                service,
                                service.copy(service.extractParamMap()),
                                stage_type.load(path),
                            )
                            for candidate in candidates:
                                param = candidate.getParam(name)
                                value = getattr(candidate, "get" + suffix)()
                                self.assertIsInstance(value, dict)
                                self.assertEqual(value, expected)
                                self.assertEqual(
                                    json.loads(json.dumps(value)), expected
                                )
                                self.assertNotIn(param, candidate.extractParamMap())

    def test_generic_enumeration_excludes_jvm_service_settings(self):
        for service in (
            TextSentiment(),
            self.analyzer().setApiVersionCol("version"),
            AnalyzeDocument()
            .setSubscriptionKey("synthetic-key")
            .setImageUrlCol("document"),
        ):
            with self.subTest(stage=type(service).__name__):
                values = {
                    param.name: service.getOrDefault(param)
                    for param in service.params
                    if service.isDefined(param)
                }
                for param in service.params:
                    if param.doc.startswith("ServiceParam:"):
                        self.assertNotIn(param.name, values)
                        self.assertNotIn(param, service.extractParamMap())
                        value = service._java_service_value(param)
                        if value is not None and value.isRight():
                            self.assertIn(value.value(), service.explainParam(param))

    def test_default_column_binding_scalar_override_and_clear(self):
        service = TextSentiment()
        param = service.text
        self.assertFalse(service.hasDefault(param))
        self.assertFalse(service.isDefined(param))
        self.assertIn("text", service.explainParam(param))
        service.setText(["synthetic text"])
        self.assertFalse(service.isSet(param))
        self.assertFalse(service.isDefined(param))
        self.assertFalse(service.hasDefault(param))
        self.assertEqual(service.getText(), ["synthetic text"])
        self.assertNotIn(param, service.extractParamMap())
        service.clear(param)
        for candidate in (service, service.copy({})):
            self.assertFalse(candidate.isSet(param))
            self.assertFalse(candidate.hasDefault(param))
            self.assertFalse(candidate.isDefined(param))
            self.assertNotIn(param, candidate.extractParamMap())
            self.assertEqual(candidate._java_service_value(param).value(), "text")
            with self.assertRaises(KeyError):
                candidate.getOrDefault(param)

    def test_saved_ordinary_defaults_survive_python_load_and_transfer(self):
        stage = ContentUnderstanding()._setDefault(
            maxOperationDuration=1000, pollingDelay=17
        )
        stage.setPollingDelay(23)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "stage")
            stage.write().save(path)
            for loaded in (
                ContentUnderstanding.load(path),
                ContentUnderstanding.load(path).copy({}),
            ):
                self.assertEqual(loaded.getMaxOperationDuration(), 1000)
                self.assertEqual(loaded.getPollingDelay(), 23)
                loaded.clear(loaded.pollingDelay)
                self.assertEqual(loaded.getPollingDelay(), 17)
                loaded._transfer_params_to_java()
                self.assertEqual(loaded._java_obj.getMaxOperationDuration(), 1000)
                self.assertEqual(loaded._java_obj.getPollingDelay(), 17)

    def test_service_parameter_inspection_preserves_scalar_and_column_state(self):
        for service in (ContentUnderstanding(), AnalyzeDocument()):
            service.setSubscriptionKey("synthetic-key")
            self.assertNotIn(
                "synthetic-key", service.explainParam(service.subscriptionKey)
            )
            self.assertIn("[redacted]", service.explainParam(service.subscriptionKey))
            self.assertNotIn("synthetic-key", service.explainParams())
            service.setSubscriptionKeyCol("credential")
            self.assertIn("credential", service.explainParam("subscriptionKey"))
            service.set(service.subscriptionKey, "pending-key")
            self.assertNotIn("pending-key", service.explainParam("subscriptionKey"))
            self.assertIn("[redacted]", service.explainParam("subscriptionKey"))
            service._transfer_params_to_java()
            self.assertNotIn("pending-key", service.explainParams())
            self.assertEqual(service.getSubscriptionKey(), "pending-key")
            self.assertNotIn(service.subscriptionKey, service.extractParamMap())
            service.setAADToken("synthetic-token")
            service.setCustomAuthHeader("synthetic-auth")
            service.setCustomHeaders({"Authorization": "synthetic-header"})
            service._java_obj.setDefaultAADToken("synthetic-default")
            service.setAADTokenCol("tokens")
            explanation = service.explainParams()
            for secret in (
                "synthetic-token",
                "synthetic-auth",
                "synthetic-header",
                "synthetic-default",
            ):
                self.assertNotIn(secret, explanation)
            self.assertIn("tokens", service.explainParam("AADToken"))

    def test_existing_service_binary_parameters_load_as_raw_bytes(self):
        for label, payload in (
            ("media", b"\x89PNG\r\n\x1a\n\x00\xff"),
            ("pickle", pickle.dumps(MediaPickleProbe())),
        ):
            with self.subTest(
                payload=label
            ), tempfile.TemporaryDirectory() as directory:
                service = AnalyzeDocument().setImageBytes(bytearray(payload))
                path = os.path.join(directory, "service")
                service.write().save(path)
                loaded = AnalyzeDocument.load(path).copy({})
                self.assertEqual(bytes(loaded.getImageBytes()), payload)
                self.assertNotIn(loaded.imageBytes, loaded.extractParamMap())
                loaded._transfer_params_to_java()
                self.assertEqual(bytes(loaded.getImageBytes()), payload)

    def test_existing_service_column_bindings_survive_load_copy_and_resave(self):
        service = (
            AnalyzeDocument()
            .setImageUrlCol("document")
            .setSubscriptionKeyCol("credential")
            .setApiVersionCol("version")
        )
        with tempfile.TemporaryDirectory() as directory:
            for attempt in range(2):
                path = os.path.join(directory, str(attempt))
                service.write().save(path)
                service = AnalyzeDocument.load(path).copy({})
                service._transfer_params_to_java()
                for name, column in (
                    ("imageUrl", "document"),
                    ("subscriptionKey", "credential"),
                    ("apiVersion", "version"),
                ):
                    value = service._java_obj.getOrDefault(
                        service._java_obj.getParam(name)
                    )
                    self.assertTrue(value.isRight())
                    self.assertEqual(value.value(), column)
                    self.assertNotIn(service.getParam(name), service.extractParamMap())
                    with self.assertRaises(KeyError):
                        service.getOrDefault(name)
                self.assertIn("document", service.explainParam("imageUrl"))

    def test_invalid_binary_rows_do_not_prevent_valid_analysis(self):
        documents = spark.createDataFrame(
            [
                ("missing", None),
                ("empty", bytearray()),
                ("valid", bytearray(b"document")),
            ],
            "documentId string, body binary",
        )
        rows = self.analyzer().transform(documents).collect()
        self.assertEqual(
            [row.analysis.status for row in rows], ["Failed", "Failed", "Succeeded"]
        )
        self.assertEqual(sum(request[0] == "POST" for request in self.requests), 1)

    def test_creation_deadline_retains_analyzer_resource_through_python_wrapper(self):
        body = {"analyzerId": "custom", "status": "creating"}
        location = (
            f"http://127.0.0.1:{self.server.server_port}"
            "/contentunderstanding/analyzers/custom/operations/create-1"
            "?api-version=2025-11-01"
        )
        requests = self.requests

        def accept_creation(handler):
            requests.append(("PUT", handler.path, handler.read_body()))
            handler.send_json(201, body, location, retry_after="120")

        self.Handler.do_PUT = accept_creation
        analyzer = (
            self.analyzer().setAnalyzerId("custom").setMaxOperationDuration(60000)
        )
        with self.assertRaises(Py4JJavaError) as raised:
            analyzer.createAnalyzer({})
        response = raised.exception.java_exception.response()
        self.assertEqual(response.status(), "Running")
        self.assertEqual(response.httpStatus(), 201)
        self.assertEqual(json.loads(response.rawResponse()), body)
        self.assertEqual(response.operationLocation().get(), location)
        self.assertTrue(response.id().isEmpty())
        self.assertEqual(
            json.loads(response.error().get())["code"], "OperationTimedOut"
        )
        self.assertEqual([request[0] for request in self.requests], ["PUT"])

    def test_analyzer_creation_requires_explicit_id_without_changing_get_default(self):
        analyzer = self.analyzer()
        for replace in (False, True):
            for cleared in (False, True):
                with self.subTest(replace=replace, cleared=cleared):
                    if cleared:
                        analyzer.setAnalyzerId("custom-v1")
                        analyzer.clear(analyzer.analyzerId)
                    with self.assertRaisesRegex(
                        IllegalArgumentException, "explicit analyzerId"
                    ):
                        analyzer.createAnalyzer(
                            {"baseAnalyzerId": "prebuilt-document"},
                            allowReplace=replace,
                        )
                    self.assertEqual(self.requests, [])
        self.assertEqual(
            json.loads(analyzer.getAnalyzer())["analyzerId"], "prebuilt-read"
        )
        self.assertEqual([request[0] for request in self.requests], ["GET"])

    def test_implicit_analyzer_id_survives_python_copy_load_and_resave(self):
        for cleared in (False, True):
            analyzer = self.analyzer().setOperationMode("submit")
            if cleared:
                analyzer.setAnalyzerId("custom")
                analyzer.clear(analyzer.analyzerId)
            with tempfile.TemporaryDirectory() as directory:
                path = os.path.join(directory, "stage")
                analyzer.copy({}).save(path)
                loaded = ContentUnderstanding.load(path)
                resaved = os.path.join(directory, "resaved")
                loaded.copy({}).save(resaved)
                pipeline_path = os.path.join(directory, "pipeline")
                Pipeline(stages=[analyzer]).fit(self.documents()).save(pipeline_path)
                pipeline_stage = PipelineModel.load(pipeline_path).stages[0]
                for candidate in (
                    loaded,
                    ContentUnderstanding.load(resaved),
                    pipeline_stage,
                ):
                    for replace in (False, True):
                        with self.assertRaisesRegex(
                            IllegalArgumentException, "explicit analyzerId"
                        ):
                            candidate.createAnalyzer({}, allowReplace=replace)
                    self.assertFalse(
                        candidate._java_obj.isSet(
                            candidate._java_obj.getParam("analyzerId")
                        )
                    )
                    self.assertEqual(candidate.getAnalyzerId(), "prebuilt-read")
                    self.assertEqual(
                        json.loads(candidate.getAnalyzer())["analyzerId"],
                        "prebuilt-read",
                    )
                    self.assertEqual(
                        candidate.transform(self.documents()).first().analysis.status,
                        "Running",
                    )
        self.assertFalse(any(request[0] == "PUT" for request in self.requests))

    def test_explicit_python_analyzer_settings_survive_load_and_clear(self):
        for analyzer_id in ("prebuilt-read", "custom"):
            analyzer = self.analyzer()
            analyzer.set(analyzer.analyzerId, analyzer_id)
            with tempfile.TemporaryDirectory() as directory:
                path = os.path.join(directory, "stage")
                analyzer.copy({}).save(path)
                loaded = ContentUnderstanding.load(path)
                for replace in (False, True):
                    self.assertEqual(
                        json.loads(loaded.createAnalyzer({}, replace))["analyzerId"],
                        analyzer_id,
                    )
                loaded.clear(loaded.analyzerId)
                resaved = os.path.join(directory, "cleared")
                loaded.save(resaved)
                restored = ContentUnderstanding.load(resaved)
                for replace in (False, True):
                    with self.assertRaisesRegex(
                        IllegalArgumentException, "explicit analyzerId"
                    ):
                        restored.createAnalyzer({}, replace)
                restored.setParams(analyzerId=analyzer_id)
                self.assertEqual(
                    json.loads(restored.createAnalyzer({}))["analyzerId"], analyzer_id
                )

    def test_legacy_saved_analyzer_settings_keep_their_management_behavior(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "legacy")
            self.analyzer().save(path)
            metadata_path = os.path.join(path, "metadata")
            metadata = json.loads(spark.read.text(metadata_path).first().value)
            # Legacy SynapseML writers materialized defaults in the explicit map.
            metadata["paramMap"].update(
                {
                    name: value
                    for name, value in metadata["defaultParamMap"].items()
                    if name not in metadata["paramMap"]
                }
            )
            spark.createDataFrame([(json.dumps(metadata),)], ["value"]).write.mode(
                "overwrite"
            ).text(metadata_path)
            loaded = ContentUnderstanding.load(path)
            resaved = os.path.join(directory, "resaved")
            loaded.save(resaved)
            loaded = ContentUnderstanding.load(resaved)
            for replace in (False, True):
                self.assertEqual(
                    json.loads(loaded.createAnalyzer({}, replace))["analyzerId"],
                    "prebuilt-read",
                )
            self.assertTrue(
                loaded._java_obj.isSet(loaded._java_obj.getParam("analyzerId"))
            )
            loaded.clear(loaded.analyzerId)
            with self.assertRaisesRegex(
                IllegalArgumentException, "explicit analyzerId"
            ):
                loaded.createAnalyzer({})

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
        set_value = self.analyzer().setDocumentBytes(payload)
        set_value.clear(set_value.documentBytes)
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
