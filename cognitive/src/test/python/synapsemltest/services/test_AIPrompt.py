# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import ast
import importlib.util
import json
import tempfile
import threading
import unittest
from contextlib import contextmanager
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

from pyspark.errors import IllegalArgumentException
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.ai import AIPrompt, AISystemOne
from synapse.ml.services.openai import OpenAIDecisions, OpenAIEmbedding, OpenAIPrompt

spark = SparkSession.getActiveSession() or init_spark()


@contextmanager
def endpoint(response, exact_numbers=False):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(
                (
                    self.path,
                    json.loads(
                        self.rfile.read(int(self.headers["Content-Length"])),
                        parse_float=Decimal if exact_numbers else float,
                    ),
                )
            )
            body = json.dumps(response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


class TestAIPrompt(unittest.TestCase):
    def test_facade_does_not_inherit_legacy_deployment_default(self):
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            with self.subTest(stage=stage_type.__name__):
                stage = stage_type(model="selected-model", inputCol="text")
                for candidate in (stage, stage.copy({})):
                    self.assertFalse(candidate.isDefined(candidate.deploymentName))
                    candidate._transfer_params_to_java()
                    self.assertFalse(
                        candidate._java_obj.isDefined(
                            candidate._java_obj.deploymentName()
                        )
                    )
                    self.assertEqual(candidate.getModel(), "selected-model")

    def test_unsupported_typed_cache_controls_reject_after_copy_and_load(self):
        data = spark.createDataFrame([("hello",)], "text STRING")
        for api in ("chat_completions", "responses"):
            with self.subTest(api=api), endpoint({}) as (
                url,
                requests,
            ), tempfile.TemporaryDirectory() as directory:
                stage = AIPrompt(
                    apiType=api, url=url + "/v1", model="model", inputCol="text"
                ).setPromptCacheOptions({"mode": "auto"})
                path = str(Path(directory) / "stage")
                stage.write().save(path)
                for candidate in (stage, stage.copy({}), AIPrompt.load(path)):
                    with self.assertRaisesRegex(
                        IllegalArgumentException, "promptCacheOptions is not supported"
                    ):
                        candidate.transform(data)
                self.assertEqual(requests, [])

    def test_http_framing_headers_reject_after_python_copy_and_persistence(self):
        data = spark.createDataFrame([("hello",)], "text STRING")
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            with self.subTest(stage=stage_type.__name__), endpoint({}) as (
                url,
                requests,
            ), tempfile.TemporaryDirectory() as directory:
                stage = stage_type(
                    url=url if stage_type is AISystemOne else url + "/v1",
                    model="model",
                    inputCol="text",
                    outputCol="answer",
                    errorCol="error",
                    rawResponseCol="raw",
                    usageCol="usage",
                    responseIdCol="response_id",
                )
                if stage_type is AISystemOne:
                    stage.setQuestions(
                        {"check": {"type": "noul", "instructions": "Check"}}
                    )
                elif stage_type is OpenAIDecisions:
                    stage.setQuestions(
                        [
                            {
                                "type": "predicate",
                                "name": "check",
                                "instructions": "Check",
                            }
                        ]
                    )
                path = str(Path(directory) / "stage")
                stage.write().save(path)
                loaded = stage_type.load(path).copy({})
                loaded.setCustomHeaders({"cOnTeNt-LeNgTh": "0"})
                with self.assertRaisesRegex(
                    IllegalArgumentException, "HTTP framing headers"
                ):
                    loaded.transform(data)
                self.assertEqual(requests, [])

    def test_explicit_java_wrappers_preserve_uid_defaults_and_parameters(self):
        names = ("outputCol", "errorCol", "messagesCol", "responseIdCol")
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            for index in range(2):
                with self.subTest(stage=stage_type.__name__, index=index):
                    java_type = spark._jvm
                    for part in stage_type.getJavaPackage().split("."):
                        java_type = getattr(java_type, part)
                    original = (
                        java_type(f"wrapped-{stage_type.__name__}-{index}")
                        .setConcurrency(index + 2)
                        .setInputCol("text")
                        .setModelCol("model")
                    )
                    defaults = {
                        name: original.getDefault(original.getParam(name)).get()
                        for name in names
                    }
                    wrapped = stage_type(java_obj=original)
                    for name, expected in defaults.items():
                        self.assertFalse(wrapped.isSet(name))
                        self.assertEqual(wrapped.getOrDefault(name), expected)
                    self.assertEqual(wrapped.uid, original.uid())
                    for candidate in (wrapped, wrapped.copy({})):
                        self.assertEqual(candidate.uid, original.uid())
                        self.assertEqual(candidate.getConcurrency(), index + 2)
                        self.assertEqual(candidate.getInputCol(), "text")
                        self.assertEqual(candidate.getModelCol(), "model")
                        candidate._transfer_params_to_java()
                        for name, expected in defaults.items():
                            self.assertFalse(candidate.isSet(name))
                            self.assertEqual(candidate.getOrDefault(name), expected)
                            self.assertEqual(
                                original.getDefault(original.getParam(name)).get(),
                                expected,
                            )

    def test_fresh_instances_preserve_their_own_java_defaults_and_explicit_columns(
        self,
    ):
        names = ("outputCol", "errorCol", "messagesCol", "responseIdCol")
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            with self.subTest(stage=stage_type.__name__):
                stages = [stage_type(), stage_type()]
                for name in names:
                    self.assertNotEqual(
                        stages[0].getOrDefault(name), stages[1].getOrDefault(name)
                    )
                    for stage in stages:
                        self.assertFalse(stage.isSet(name))
                        self.assertEqual(
                            stage.getOrDefault(name),
                            stage._java_obj.getOrDefault(
                                stage._java_obj.getParam(name)
                            ),
                        )
                explicit = {name: "explicit_" + name for name in names}
                stage = stage_type(**explicit)
                for name, value in explicit.items():
                    self.assertTrue(stage.isSet(name))
                    self.assertEqual(stage.getOrDefault(name), value)
                    stage.clear(stage.getParam(name))
                    self.assertFalse(stage.isSet(name))
                    self.assertEqual(
                        stage.getOrDefault(name),
                        stage._java_obj.getDefault(
                            stage._java_obj.getParam(name)
                        ).get(),
                    )

    def test_fresh_pipeline_stages_retain_both_default_outputs(self):
        data = spark.createDataFrame([("hello",)], "text STRING")
        replies = {
            "chat_completions": {
                "choices": [
                    {"index": 0, "message": {"role": "assistant", "content": "answer"}}
                ]
            },
            "responses": {
                "id": "response-id",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "answer"}],
                    }
                ],
            },
        }
        for api, reply in replies.items():
            with self.subTest(api=api), endpoint(reply) as (url, requests):
                stages = [
                    AIPrompt(
                        apiType=api,
                        url=url + "/v1",
                        model="model",
                        inputCol="text",
                        dropPrompt=False,
                    )
                    for _ in range(2)
                ]
                if api == "responses":
                    for stage in stages:
                        stage.setStore(True)
                result = Pipeline(stages=stages).fit(data).transform(data)
                row = result.head()
                self.assertEqual(len(requests), 2)
                for name in ("outputCol", "errorCol", "messagesCol") + (
                    ("responseIdCol",) if api == "responses" else ()
                ):
                    columns = [stage.getOrDefault(name) for stage in stages]
                    self.assertEqual(len(set(columns)), 2)
                    for column in columns:
                        self.assertIn(column, result.columns)
                        if name == "outputCol":
                            self.assertEqual(row[column], "answer")
                        elif name == "errorCol":
                            self.assertIsNone(row[column])
                        elif name == "messagesCol":
                            self.assertEqual(
                                [
                                    message["content"][0]["text"]
                                    for message in row[column]
                                    if message["role"] == "user"
                                ],
                                ["hello"],
                            )
                        else:
                            self.assertEqual(row[column], "response-id")

    def test_java_parameter_transfer_preserves_include_defaults_argument(self):
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            for include_defaults in (False, True, None):
                for keyword in (False, True):
                    with self.subTest(
                        stage=stage_type.__name__,
                        include_defaults=include_defaults,
                        keyword=keyword,
                    ):
                        stage = stage_type()
                        java_default = stage.getConcurrency()
                        stage._setDefault(concurrency=java_default + 1)
                        if include_defaults is None:
                            stage._transfer_params_from_java()
                        elif keyword:
                            stage._transfer_params_from_java(
                                include_defaults=include_defaults
                            )
                        else:
                            stage._transfer_params_from_java(include_defaults)
                        self.assertEqual(
                            stage.getConcurrency(),
                            java_default + 1
                            if include_defaults is False
                            else java_default,
                        )
                        self.assertFalse(stage.isSet(stage.concurrency))
                        stage.setConcurrency(java_default + 2)
                        stage._transfer_params_to_java()
                        stage._transfer_params_from_java()
                        self.assertEqual(stage.getConcurrency(), java_default + 2)
                        self.assertTrue(stage.isSet(stage.concurrency))

    def test_native_loader_preserves_json_and_runtime_class(self):
        value = '{"decimal":0.12345678901234567890123456789,"large":1e400}'
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            with self.subTest(
                stage=stage_type.__name__
            ), tempfile.TemporaryDirectory() as directory:
                original = stage_type().setExtraBody(value)
                saved = str(Path(directory) / "stage")
                original.write().save(saved)
                for reader in (stage_type, AIPrompt):
                    loaded = reader.load(saved)
                    self.assertIs(type(loaded), stage_type)
                    for candidate in (loaded, loaded.copy({})):
                        candidate._transfer_params_to_java()
                        restored = candidate._java_obj
                        self.assertEqual(candidate.getApiType(), original.getApiType())
                        self.assertEqual(
                            restored.getClass().getName(),
                            original._java_obj.getClass().getName(),
                        )
                        self.assertEqual(
                            restored.extraBody().jsonEncode(
                                restored.getOrDefault(restored.extraBody())
                            ),
                            original._java_obj.extraBody().jsonEncode(
                                original._java_obj.getOrDefault(
                                    original._java_obj.extraBody()
                                )
                            ),
                        )
                        self.assertTrue(restored.isSet(restored.extraBody()))
                        self.assertFalse(restored.isSet(restored.temperature()))

    def test_json_parameter_getters_preserve_exact_number_roundtrips(self):
        numbers = (
            '{"precise":0.12345678901234567890123456789,"tiny":1e-400,'
            '"huge":1e400,"integer":10000000000000000000000000000000000000000,'
            '"ordinary":0.1,"half":0.5,"nested":[0.12345678901234567890123456789]}'
        )
        values = {
            "extraBody": '{"values":' + numbers + "}",
            "responseFormat": '{"type":"json_schema","name":"answer","schema":'
            + '{"type":"object","properties":{"value":{"type":"number"}},"values":'
            + numbers
            + "}}",
            "questions": '[{"type":"predicate","instructions":"Check","values":'
            + numbers
            + "}]",
        }
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            for name, text in values.items():
                with self.subTest(
                    stage=stage_type.__name__, parameter=name
                ), tempfile.TemporaryDirectory() as directory:
                    suffix = name[0].upper() + name[1:]
                    original = stage_type()
                    parameter = original._java_obj.getParam(name)
                    if name == "responseFormat":
                        decoded = parameter.jsonDecode('{"left":' + text + "}")
                        original._java_obj.setResponseFormat(decoded.left().get())
                    else:
                        getattr(original, "set" + suffix)(text)
                    encoded = parameter.jsonEncode(
                        original._java_obj.getOrDefault(parameter)
                    )
                    path = str(Path(directory) / "stage")
                    original.write().save(path)
                    for candidate in (
                        original,
                        original.copy({}),
                        stage_type.load(path),
                    ):
                        value = getattr(candidate, "get" + suffix)()
                        container = (
                            value["json_schema"]["schema"]
                            if name == "responseFormat"
                            else value[0]
                            if name == "questions"
                            else value
                        )
                        actual = container["values"]
                        for key in ("precise", "tiny", "huge"):
                            self.assertIsInstance(actual[key], Decimal)
                        self.assertEqual(
                            actual["precise"],
                            Decimal("0.12345678901234567890123456789"),
                        )
                        self.assertEqual(actual["tiny"], Decimal("1e-400"))
                        self.assertEqual(actual["huge"], Decimal("1e400"))
                        self.assertEqual(actual["nested"], [actual["precise"]])
                        self.assertIsInstance(actual["integer"], int)
                        self.assertIsInstance(actual["ordinary"], float)
                        self.assertEqual(actual["ordinary"], 0.1)
                        self.assertIsInstance(actual["half"], float)
                        for rebound in (
                            getattr(candidate, "set" + suffix)(value),
                            stage_type(**{name: value}),
                            stage_type().setParams(**{name: value}),
                        ):
                            java_parameter = rebound._java_obj.getParam(name)
                            self.assertEqual(
                                java_parameter.jsonEncode(
                                    rebound._java_obj.getOrDefault(java_parameter)
                                ),
                                encoded,
                            )

    def test_json_parameters_reject_nonfinite_decimals_and_cycles_atomically(self):
        cycle = []
        cycle.append(cycle)
        for name in ("extraBody", "responseFormat", "questions"):
            suffix = name[0].upper() + name[1:]
            stage = getattr(AIPrompt(), "set" + suffix)({"type": "json_object"})
            for per_row in (False, True):
                if per_row:
                    getattr(stage, "set" + suffix + "Col")("values")
                parameter = stage._java_obj.getParam(name)
                original = parameter.jsonEncode(stage._java_obj.getOrDefault(parameter))
                for invalid in (
                    Decimal("NaN"),
                    Decimal("sNaN"),
                    Decimal("Infinity"),
                    Decimal("-Infinity"),
                    cycle,
                ):
                    with self.subTest(
                        parameter=name, per_row=per_row, invalid=type(invalid).__name__
                    ):
                        with self.assertRaises(ValueError):
                            getattr(stage, "set" + suffix)({"invalid": invalid})
                        self.assertEqual(
                            parameter.jsonEncode(
                                stage._java_obj.getOrDefault(parameter)
                            ),
                            original,
                        )

    def test_json_surrogates_reject_invalid_parameters_and_preserve_saved_unicode(self):
        unicode = "caf\u00e9 \u96ea \U0001f600 \ufffd"
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            stage = stage_type()
            for name in ("extraBody", "responseFormat", "questions"):
                suffix = name[0].upper() + name[1:]
                setter = getattr(stage, "set" + suffix)
                setter({"type": "json_object"})
                parameter = stage._java_obj.getParam(name)
                original = parameter.jsonEncode(stage._java_obj.getOrDefault(parameter))
                for malformed in ("\ud800", "\udc00", "\ud800x", "\udc00\ud800"):
                    for payload in (
                        {"private-field": malformed},
                        {malformed: "private-value", "?": 2},
                    ):
                        values = (
                            (payload,)
                            if name == "responseFormat"
                            else (payload, json.dumps(payload))
                        )
                        for value in values:
                            with self.subTest(
                                stage=stage_type.__name__, parameter=name
                            ):
                                with self.assertRaisesRegex(
                                    IllegalArgumentException, "surrogate"
                                ):
                                    setter(value)
                                self.assertEqual(
                                    parameter.jsonEncode(
                                        stage._java_obj.getOrDefault(parameter)
                                    ),
                                    original,
                                )
            stage.setExtraBody({unicode: [unicode]})
            with tempfile.TemporaryDirectory() as directory:
                stage.write().save(directory + "/stage")
                for candidate in (
                    stage.copy({}),
                    stage_type.load(directory + "/stage"),
                ):
                    self.assertEqual(candidate.getExtraBody(), {unicode: [unicode]})

    def test_json_surrogates_remain_row_errors_without_corrupting_requests_or_answers(
        self,
    ):
        reply = {
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "\U0001f600"}}
            ]
        }
        bodies = [
            json.dumps({"private-field": "\ud800"}),
            json.dumps({"\udc00": "private-value", "?": 2}),
            json.dumps({"\U0001f600": "\U0001f600", "?": 2}),
        ]
        data = spark.createDataFrame(
            [(index, "hello", body) for index, body in enumerate(bodies)],
            "id INT, text STRING, body STRING",
        )
        with endpoint(reply) as (url, requests):
            stage = AIPrompt(
                url=url + "/v1",
                model="model",
                inputCol="text",
                extraBodyCol="body",
                outputCol="answer",
                errorCol="error",
            )
            rows = sorted(stage.transform(data).collect(), key=lambda row: row.id)
            for row in rows[:2]:
                self.assertIsNone(row.answer)
                self.assertEqual(
                    json.loads(row.error.response)["code"], "invalid_request"
                )
            self.assertIsNone(rows[2].error)
            self.assertEqual(rows[2].answer, "\U0001f600")
            self.assertEqual(len(requests), 1)
            self.assertEqual(requests[0][1]["\U0001f600"], "\U0001f600")
            self.assertEqual(requests[0][1]["?"], 2)
        for malformed in ("\ud800", "\udc00"):
            reply["choices"][0]["message"]["content"] = malformed
            with endpoint(reply) as (url, requests):
                stage = AIPrompt(
                    url=url + "/v1",
                    model="model",
                    inputCol="text",
                    outputCol="answer",
                    errorCol="error",
                    rawResponseCol="raw",
                )
                row = stage.transform(data.limit(1)).head()
                self.assertIsNone(row.answer)
                self.assertEqual(json.loads(row.error.response)["code"], "parse_error")
                self.assertEqual(row.raw, json.dumps(reply))
                self.assertEqual(len(requests), 1)

    def test_scalar_header_preflight_rejects_configuration_without_sending_requests(
        self,
    ):
        with endpoint({}) as (url, requests):
            data = spark.createDataFrame([("hello",)], "text STRING")
            for configuration in (
                {"subscriptionKey": "synthetic-private\n"},
                {"AADToken": "synthetic-private\n"},
                {"customHeaders": {"X-Caller": "synthetic-private\n"}},
            ):
                stage = AIPrompt(
                    url=url + "/v1", model="model", inputCol="text", **configuration
                )
                with self.assertRaisesRegex(
                    IllegalArgumentException, "Invalid HTTP header"
                ) as error:
                    stage.transform(data)
                self.assertNotIn("synthetic-private", str(error.exception))
            self.assertEqual(requests, [])

    def test_mixed_header_preflight_validates_scalar_fallback_only_when_selected(self):
        reply = {
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "answer"}}
            ]
        }
        data = spark.createDataFrame(
            [("valid", "synthetic-key"), ("null", None), ("blank", " ")],
            "text STRING, key STRING",
        )
        with endpoint(reply) as (url, requests):
            stage = AIPrompt(
                url=url + "/v1",
                model="model",
                inputCol="text",
                subscriptionKeyCol="key",
                AADToken="synthetic-private\n",
                outputCol="answer",
                errorCol="error",
            )
            rows = {row.text: row for row in stage.transform(data).collect()}
            self.assertIsNone(rows["valid"].error)
            self.assertEqual(rows["valid"].answer, "answer")
            for name in ("null", "blank"):
                self.assertIsNone(rows[name].answer)
                self.assertEqual(
                    json.loads(rows[name].error.response)["code"], "invalid_request"
                )
                self.assertNotIn("synthetic-private", rows[name].error.response)
            self.assertEqual(len(requests), 1)

    def test_structured_json_surrogates_produce_diagnostics_after_provider_decode(self):
        data = spark.createDataFrame([("hello",)], "text STRING")
        documents = (
            {"value": "\ud800"},
            {"\udc00": "private-value", "?": "other"},
            {"value": "\U0001f600", "\U0001f600": "unchanged"},
        )
        for api in ("chat_completions", "responses"):
            for index, document in enumerate(documents):
                text = json.dumps(document)
                reply = (
                    {
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": text},
                            }
                        ]
                    }
                    if api == "chat_completions"
                    else {
                        "output": [
                            {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": text}],
                            }
                        ]
                    }
                )
                with self.subTest(api=api, document=index), endpoint(reply) as (
                    url,
                    requests,
                ):
                    stage = AIPrompt(
                        apiType=api,
                        url=url + "/v1",
                        model="model",
                        inputCol="text",
                        outputCol="answer",
                        errorCol="error",
                        rawResponseCol="raw",
                        postProcessingOptions={"jsonSchema": "MAP<STRING,STRING>"},
                    )
                    row = stage.transform(data).head()
                    if index < 2:
                        self.assertIsNotNone(row.error)
                        self.assertEqual(
                            json.loads(row.error.response)["code"], "parse_error"
                        )
                        self.assertNotIn("private-value", row.error.response)
                    else:
                        self.assertIsNone(row.error)
                        self.assertEqual(row.answer, document)
                    self.assertEqual(row.raw, json.dumps(reply))
                    self.assertEqual(len(requests), 1)

    def test_leaf_loaders_reject_incompatible_saved_classes(self):
        for original in (AIPrompt(), AISystemOne(), OpenAIDecisions()):
            with self.subTest(
                stage=type(original).__name__
            ), tempfile.TemporaryDirectory() as directory:
                saved = str(Path(directory) / "stage")
                original.write().save(saved)
                loaded = AIPrompt.load(saved)
                self.assertEqual(
                    loaded._java_obj.getClass().getName(),
                    original._java_obj.getClass().getName(),
                )
                for reader in (AISystemOne, OpenAIDecisions):
                    if not isinstance(original, reader):
                        with self.assertRaisesRegex(
                            IllegalArgumentException, "incompatible"
                        ):
                            reader.load(saved)

    def test_public_stub_methods_exist_on_the_native_runtime_classes(self):
        for stage_class in (AIPrompt, AISystemOne, OpenAIDecisions):
            with self.subTest(stage=stage_class.__name__):
                module = importlib.import_module(stage_class.__module__)
                stub = ast.parse(Path(module.__file__).with_suffix(".pyi").read_text())
                definition = next(
                    node
                    for node in stub.body
                    if isinstance(node, ast.ClassDef)
                    and node.name == stage_class.__name__
                )
                for method in definition.body:
                    if isinstance(
                        method, ast.FunctionDef
                    ) and not method.name.startswith("_"):
                        self.assertTrue(
                            callable(getattr(stage_class, method.name, None)),
                            f"{stage_class.__name__}.{method.name} is stub-only",
                        )

    def test_json_object_stub_inputs_accept_strings_and_dicts(self):
        for stage_class in (AIPrompt, AISystemOne, OpenAIDecisions):
            with self.subTest(stage=stage_class.__name__):
                module = importlib.import_module(stage_class.__module__)
                stub = ast.parse(Path(module.__file__).with_suffix(".pyi").read_text())
                definition = next(
                    node
                    for node in stub.body
                    if isinstance(node, ast.ClassDef)
                    and node.name == stage_class.__name__
                )
                methods = {
                    node.name: node
                    for node in definition.body
                    if isinstance(node, ast.FunctionDef)
                }
                input_type = "Union[str, Dict[str, Any]]"
                for param in ("responseFormat", "extraBody"):
                    suffix = param[0].upper() + param[1:]
                    for method in ("__init__", "setParams"):
                        arguments = {
                            argument.arg: ast.unparse(argument.annotation)
                            for argument in methods[method].args.kwonlyargs
                        }
                        self.assertEqual(arguments[param], f"Optional[{input_type}]")
                        self.assertEqual(arguments[f"{param}Col"], "Optional[str]")
                    self.assertEqual(
                        ast.unparse(methods[f"set{suffix}"].args.args[1].annotation),
                        input_type,
                    )
                    self.assertEqual(
                        ast.unparse(methods[f"get{suffix}"].returns), "Dict[str, Any]"
                    )
                    self.assertEqual(
                        ast.unparse(methods[f"set{suffix}Col"].args.args[1].annotation),
                        "str",
                    )
                    self.assertIn(f"{param} ({input_type})", stage_class.__doc__)

    def test_column_types_reject_unsupported_kinds_before_http(self):
        data = spark.createDataFrame([("attachment.txt",)], "asset STRING")
        for api in ("chat_completions", "responses"):
            for binding in ("constructor", "setParams", "setter"):
                with self.subTest(api=api, binding=binding), endpoint({}) as (
                    url,
                    requests,
                ):
                    arguments = {
                        "apiType": api,
                        "url": url + "/v1",
                        "model": "model",
                        "promptTemplate": "{asset}",
                    }
                    kinds = {"asset": "image"}
                    if binding == "constructor":
                        arguments["columnTypes"] = kinds
                    stage = AIPrompt(**arguments)
                    if binding == "setParams":
                        stage.setParams(columnTypes=kinds)
                    elif binding == "setter":
                        stage.setColumnTypes(kinds)
                    with self.assertRaisesRegex(
                        IllegalArgumentException, "columnTypes"
                    ):
                        stage.transform(data)
                    self.assertEqual(requests, [])

    def test_attachment_size_limits_reject_invalid_bindings_before_http(self):
        data = spark.createDataFrame(
            [("data:text/plain;base64,aGVsbG8=",)], "asset STRING"
        )
        with endpoint({}) as (
            url,
            requests,
        ):
            for api in ("chat_completions", "responses"):
                for binding in ("constructor", "setParams", "setter", "default"):
                    for limit in (0.0, -1.0, float("nan")):
                        with self.subTest(api=api, binding=binding, limit=limit):
                            arguments = {
                                "apiType": api,
                                "url": url + "/v1",
                                "model": "model",
                                "promptTemplate": "{asset}",
                                "columnTypes": {"asset": "path"},
                            }
                            if binding == "constructor":
                                arguments["fileSizeLimitMB"] = limit
                            stage = AIPrompt(**arguments)
                            if binding == "setParams":
                                stage.setParams(fileSizeLimitMB=limit)
                            elif binding == "setter":
                                stage.setFileSizeLimitMB(limit)
                            elif binding == "default":
                                stage._setDefault(fileSizeLimitMB=limit)
                            with self.assertRaisesRegex(
                                IllegalArgumentException,
                                "fileSizeLimitMB must be positive",
                            ):
                                stage.transform(data)
                            self.assertEqual(requests, [])

    def test_attachment_size_defaults_apply_without_overriding_explicit_values(self):
        data = spark.createDataFrame(
            [("data:text/plain;base64,aGVsbG8=",)], "asset STRING"
        )
        replies = {
            "chat_completions": {
                "choices": [
                    {"index": 0, "message": {"role": "assistant", "content": "answer"}}
                ]
            },
            "responses": {
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "answer"}],
                    }
                ]
            },
        }
        for api, reply in replies.items():
            with self.subTest(api=api), endpoint(reply) as (url, requests):
                stage = AIPrompt(
                    apiType=api,
                    url=url + "/v1",
                    model="model",
                    promptTemplate="{asset}",
                    columnTypes={"asset": "path"},
                    errorCol="error",
                )._setDefault(fileSizeLimitMB=0.000001)
                row = stage.transform(data).head()
                self.assertIsNotNone(row.error)
                self.assertEqual(
                    json.loads(row.error.response)["code"], "invalid_request"
                )
                self.assertFalse(stage.isSet(stage.fileSizeLimitMB))
                self.assertEqual(requests, [])
                stage.setFileSizeLimitMB(0.001)
                self.assertIsNone(stage.transform(data).head().error)
                self.assertEqual(len(requests), 1)

    def test_fabric_qualification_rejects_optimized_python_before_setup(self):
        path = Path(__file__).with_name("aiprompt_fabric_e2e.py")
        for optimize in (1, 2):
            scenario = ModuleType("aiprompt_fabric_optimized")
            exec(
                compile(
                    path.read_text(encoding="utf-8"),
                    str(path),
                    "exec",
                    optimize=optimize,
                ),
                scenario.__dict__,
            )
            with self.subTest(optimize=optimize), mock.patch.object(
                scenario, "SparkSession"
            ) as spark_type, mock.patch.object(
                scenario.argparse.ArgumentParser, "parse_args"
            ), mock.patch.object(
                scenario, "ThreadingHTTPServer"
            ) as server, mock.patch.object(
                scenario, "threading"
            ) as worker:
                spark_type.builder.getOrCreate.side_effect = AssertionError(
                    "Spark setup must not be reached"
                )
                with self.assertRaisesRegex(RuntimeError, "requires Python assertions"):
                    scenario.main()
                spark_type.builder.getOrCreate.assert_not_called()
                server.assert_not_called()
                worker.Thread.assert_not_called()

    def test_fabric_jar_provenance_rejects_unexpected_artifacts_before_setup(self):
        path = Path(__file__).with_name("aiprompt_fabric_e2e.py")
        spec = importlib.util.spec_from_file_location(
            "aiprompt_fabric_provenance", path
        )
        scenario = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(scenario)
        expected = {
            "expected_core_jar": "candidate-core.jar",
            "expected_cognitive_jar": "candidate-cognitive.jar",
            "expected_tests_jar": "candidate-tests.jar",
        }
        sources = {
            "org.apache.spark.ml.ExplicitComplexParamsWritable": "expected_core_jar",
            "com.microsoft.azure.synapse.ml.services.ai.AIPrompt": "expected_cognitive_jar",
            "com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt": "expected_cognitive_jar",
            "com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricProbe": "expected_tests_jar",
        }
        for name, argument in sources.items():
            basename = expected[argument]
            for actual, empty in (
                (f"file:/jars/backup-{basename}", False),
                (f"file:/jars/{basename}.backup", False),
                (f"file:/jars/{basename}/other.jar", False),
                (f"file:/jars/other%2F{basename}", False),
                (f"file:/jars/{basename}", True),
            ):
                args = SimpleNamespace(**expected)
                if empty:
                    setattr(args, argument, "")
                with self.subTest(
                    source=name, actual=actual, empty=empty
                ), mock.patch.object(
                    scenario, "SparkSession"
                ) as spark_type, mock.patch.object(
                    scenario.argparse.ArgumentParser, "parse_args", return_value=args
                ), mock.patch.object(
                    scenario, "ThreadingHTTPServer"
                ) as server, mock.patch.object(
                    scenario, "threading"
                ) as worker:
                    session = spark_type.builder.getOrCreate.return_value
                    classes = {}
                    for class_name, expected_argument in sources.items():
                        java_class = mock.Mock()
                        location = (
                            java_class.getProtectionDomain.return_value.getCodeSource.return_value.getLocation.return_value
                        )
                        location.toString.return_value = (
                            actual
                            if class_name == name
                            else f"file:/jars/{expected[expected_argument]}"
                        )
                        classes[class_name] = java_class
                    session._jvm.java.lang.Class.forName.side_effect = (
                        lambda class_name, *unused: classes[class_name]
                    )
                    with self.assertRaises(AssertionError):
                        scenario.main()
                    session._jvm.org.apache.hadoop.fs.Path.assert_not_called()
                    server.assert_not_called()
                    worker.Thread.assert_not_called()

    def test_fabric_route_probe_failure_does_not_start_http_server(self):
        path = Path(__file__).with_name("aiprompt_fabric_e2e.py")
        spec = importlib.util.spec_from_file_location("aiprompt_fabric_setup", path)
        scenario = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(scenario)
        args = SimpleNamespace(
            expected_core_jar="candidate build.jar",
            expected_cognitive_jar="candidate build.jar",
            expected_tests_jar="candidate build.jar",
        )
        with mock.patch.object(
            scenario, "SparkSession"
        ) as spark_type, mock.patch.object(
            scenario.argparse.ArgumentParser, "parse_args", return_value=args
        ), mock.patch.object(
            scenario, "ThreadingHTTPServer"
        ) as server, mock.patch.object(
            scenario, "threading"
        ) as worker:
            session = spark_type.builder.getOrCreate.return_value
            java_class = session._jvm.java.lang.Class.forName.return_value
            location = (
                java_class.getProtectionDomain.return_value.getCodeSource.return_value.getLocation.return_value
            )
            location.toString.return_value = "file:/jars/candidate%20build.jar"
            probe = (
                session._jvm.com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricProbe
            )
            probe.verifyImplicitEndpoints.side_effect = RuntimeError("Probe failed")
            with self.assertRaisesRegex(RuntimeError, "Probe failed"):
                scenario.main()
            probe.verifyImplicitEndpoints.assert_called_once()
            server.assert_not_called()
            worker.Thread.assert_not_called()

    def test_jvm_saved_defaults_survive_python_loading_copy_and_transform(self):
        cases = [
            (
                AIPrompt,
                "chat_completions",
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
                AIPrompt,
                "responses",
                {
                    "id": "response-id",
                    "status": "completed",
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "answer"}],
                        }
                    ],
                },
            ),
            (
                AISystemOne,
                "systemone",
                {"model": "model", "answers": {"urgent": {"result": True}}},
            ),
            (
                OpenAIDecisions,
                "decisions",
                {
                    "model": "model",
                    "answers": [{"type": "predicate", "probability": 0.9}],
                },
            ),
        ]
        data = spark.createDataFrame([("hello",)], "text STRING")
        for stage_type, api, reply in cases:
            with self.subTest(api=api), endpoint(reply) as (url, requests):
                java_type = spark._jvm
                for part in stage_type.getJavaPackage().split("."):
                    java_type = getattr(java_type, part)
                original = (
                    java_type("saved-" + api)
                    .setApiType(api)
                    .setUrl(url if api == "systemone" else url + "/v1")
                    .setModel("model")
                    .setInputCol("text")
                )
                if api in ("chat_completions", "responses"):
                    original.setDropPrompt(False)
                if api == "responses":
                    original.setStore(True)
                elif api == "systemone":
                    original.setQuestions(
                        '{"urgent":{"type":"noul","instructions":"Is it urgent?"}}'
                    )
                elif api == "decisions":
                    original.setQuestions(
                        '[{"type":"predicate","instructions":"Is it urgent?"}]'
                    )
                defaults = {
                    name: original.getOrDefault(original.getParam(name))
                    for name in (
                        "outputCol",
                        "errorCol",
                        "messagesCol",
                        "responseIdCol",
                    )
                }
                expected_schema = StructType.fromJson(
                    json.loads(original.transformSchema(data._jdf.schema()).json())
                )
                with tempfile.TemporaryDirectory() as directory:
                    original.write().save(directory + "/jvm")
                    loaded = stage_type.load(directory + "/jvm")
                    for candidate in (loaded, loaded.copy({})):
                        for name in ("columnTypes", "postProcessingOptions"):
                            value = candidate.getOrDefault(name)
                            self.assertIsInstance(value, dict)
                            self.assertEqual(value, {})
                            self.assertFalse(value)
                            self.assertFalse(candidate.isSet(name), name)
                        for name, expected in defaults.items():
                            self.assertFalse(candidate.isSet(name), name)
                            self.assertEqual(candidate.getOrDefault(name), expected)
                        result = candidate.transform(data)
                        self.assertEqual(result.schema, expected_schema)
                        self.assertIsNone(result.head()[defaults["errorCol"]])
                        for name, expected in defaults.items():
                            java_param = candidate._java_obj.getParam(name)
                            self.assertFalse(candidate._java_obj.isSet(java_param))
                            self.assertEqual(
                                candidate._java_obj.getDefault(java_param).get(),
                                expected,
                            )
                        candidate.setOutputCol("temporary_output")
                        candidate.clear(candidate.outputCol)
                        self.assertEqual(
                            candidate.getOutputCol(), defaults["outputCol"]
                        )
                self.assertEqual(len(requests), 2)

    def test_ordered_response_schema_preserves_exact_numbers_through_persistence_and_http(
        self,
    ):
        names = ["e_one", "d_two", "c_three", "b_four", "a_five", "z_six"]
        cases = [
            (
                "chat_completions",
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
                "responses",
                {
                    "status": "completed",
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "answer"}],
                        }
                    ],
                },
            ),
        ]
        for api, reply in cases:
            for binding in ("schema", "setter", "constructor", "setParams"):
                order = (
                    names
                    if binding in ("schema", "constructor")
                    else list(reversed(names))
                )
                with self.subTest(api=api, binding=binding), endpoint(
                    reply, exact_numbers=True
                ) as (
                    url,
                    requests,
                ):
                    schema = {
                        "type": "object",
                        "properties": {
                            name: {
                                "type": "number",
                                "maximum": 10**40 + index,
                                "minimum": Decimal("0.12345678901234567890123456789"),
                            }
                            for index, name in enumerate(order)
                        },
                        "required": names,
                        "additionalProperties": False,
                    }
                    response_format = {
                        "type": "json_schema",
                        "name": "response_schema",
                        "schema": schema,
                        "strict": True,
                    }
                    arguments = dict(apiType=api, url=url + "/v1", model="model")
                    if binding == "constructor":
                        arguments["responseFormat"] = response_format
                    stage = (
                        AIPrompt(**arguments).setInputCol("text").setErrorCol("error")
                    )
                    stage.setExtraBody({name: name for name in order})
                    if binding == "schema":
                        stage.setResponseSchema(schema)
                    elif binding == "setter":
                        stage.setResponseFormat(response_format)
                    elif binding == "setParams":
                        stage.setParams(responseFormat=response_format)
                    stage.setResponseFormat(stage.getResponseFormat())
                    with tempfile.TemporaryDirectory() as directory:
                        stage.copy({}).write().save(directory + "/ordered")
                        loaded = AIPrompt.load(directory + "/ordered")
                        self.assertEqual(list(loaded.getExtraBody()), order)
                        row = loaded.transform(
                            spark.createDataFrame([("hello",)], "text STRING")
                        ).head()
                    self.assertIsNone(row.error)
                    self.assertEqual(len(requests), 1)
                    body = requests[0][1]
                    response_format = (
                        body["response_format"]["json_schema"]
                        if api == "chat_completions"
                        else body["text"]["format"]
                    )
                    self.assertEqual(
                        list(response_format["schema"]["properties"]), order
                    )
                    self.assertEqual(response_format["schema"], schema)

    def test_response_format_rejects_non_json_values_without_changing_bindings(self):
        for invalid in (
            {"nested": {1: "value"}},
            {"maximum": object()},
        ):
            response_format = {
                "type": "json_schema",
                "name": "answer",
                "schema": invalid,
            }
            with self.subTest(invalid=invalid, binding="constructor"):
                with self.assertRaises(TypeError):
                    AIPrompt(responseFormat=response_format)
            for binding in ("schema", "setter", "setParams"):
                for per_row in (False, True):
                    with self.subTest(
                        invalid=invalid, binding=binding, per_row=per_row
                    ):
                        stage = AIPrompt(responseFormat={"type": "json_object"})
                        if per_row:
                            stage.setResponseFormatCol("format")
                        original = stage._java_obj.responseFormat().jsonEncode(
                            stage._java_obj.getOrDefault(
                                stage._java_obj.responseFormat()
                            )
                        )
                        with self.assertRaises(TypeError):
                            if binding == "schema":
                                stage.setResponseSchema(invalid)
                            elif binding == "setter":
                                stage.setResponseFormat(response_format)
                            else:
                                stage.setParams(responseFormat=response_format)
                        self.assertEqual(
                            stage._java_obj.responseFormat().jsonEncode(
                                stage._java_obj.getOrDefault(
                                    stage._java_obj.responseFormat()
                                )
                            ),
                            original,
                        )

    def test_response_format_metadata_survives_python_bindings_and_persistence(self):
        details = {
            "name": "answer",
            "description": "Extract the answer",
            "schema": {"type": "object", "properties": {"answer": {"type": "string"}}},
            "strict": True,
            "provider_extension": {"value": 0.7},
        }
        nested = {"type": "json_schema", "json_schema": details}
        flat = {"type": "json_schema", **details}
        reply = {
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "answer"}}
            ],
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "answer"}],
                }
            ],
        }
        data = spark.createDataFrame(
            [("hello", json.dumps(flat))], "text STRING, format STRING"
        )
        for api in ("chat_completions", "responses"):
            for binding in ("constructor", "setter", "column"):
                with self.subTest(api=api, binding=binding), endpoint(reply) as (
                    url,
                    requests,
                ), tempfile.TemporaryDirectory() as directory:
                    arguments = {
                        "apiType": api,
                        "url": url + "/v1",
                        "model": "model",
                        "inputCol": "text",
                        "outputCol": "answer",
                        "errorCol": "error",
                    }
                    if binding == "constructor":
                        arguments["responseFormat"] = nested
                    stage = AIPrompt(**arguments)
                    if binding == "setter":
                        stage.setResponseFormat(nested)
                    elif binding == "column":
                        stage.setResponseFormatCol("format")
                    saved = str(Path(directory) / "stage")
                    stage.copy({}).write().save(saved)
                    row = AIPrompt.load(saved).transform(data).head()
                    self.assertIsNone(row.error)
                    self.assertEqual(row.answer, "answer")
                    self.assertEqual(len(requests), 1)
                    body = requests[0][1]
                    actual = (
                        body["response_format"]
                        if api == "chat_completions"
                        else body["text"]["format"]
                    )
                    self.assertEqual(
                        actual, nested if api == "chat_completions" else flat
                    )

    def test_response_format_string_shorthand_preserves_legacy_bindings(self):
        reply = {
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "answer"}}
            ],
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "answer"}],
                }
            ],
        }
        data = spark.createDataFrame([("hello",)], "text STRING")
        for api in ("chat_completions", "responses"):
            for binding in ("constructor", "setter", "setParams"):
                for selector in ("text", " JSON_OBJECT \n"):
                    with self.subTest(
                        api=api, binding=binding, selector=selector
                    ), endpoint(reply) as (
                        url,
                        requests,
                    ), tempfile.TemporaryDirectory() as directory:
                        arguments = dict(
                            apiType=api,
                            url=url + "/v1",
                            model="model",
                            inputCol="text",
                            errorCol="error",
                        )
                        if binding == "constructor":
                            arguments["responseFormat"] = selector
                        stage = AIPrompt(**arguments)
                        if binding == "setter":
                            stage.setResponseFormat(selector)
                        elif binding == "setParams":
                            stage.setParams(responseFormat=selector)
                        stage.setResponseFormat(" \n ")
                        stage.copy({}).write().save(directory + "/stage")
                        self.assertIsNone(
                            AIPrompt.load(directory + "/stage")
                            .transform(data)
                            .head()
                            .error
                        )
                        self.assertEqual(len(requests), 1)
                        body = requests[0][1]
                        actual = (
                            body["response_format"]
                            if api == "chat_completions"
                            else body["text"]["format"]
                        )
                        self.assertEqual(actual, {"type": selector.strip().lower()})
        for value in ("json_schema", '{"type":"json_object"}', "unknown"):
            with self.subTest(invalid=value):
                with self.assertRaises(IllegalArgumentException):
                    AIPrompt(responseFormat=value)
                for per_row in (False, True):
                    stage = AIPrompt(responseFormat={"type": "text"})
                    if per_row:
                        stage.setResponseFormatCol("format")
                    for set_value in (
                        stage.setResponseFormat,
                        lambda value: stage.setParams(responseFormat=value),
                    ):
                        with self.assertRaises(IllegalArgumentException):
                            set_value(value)
                        if per_row:
                            self.assertEqual(stage.getResponseFormatCol(), "format")
                        else:
                            self.assertEqual(
                                stage.getResponseFormat(), {"type": "text"}
                            )

    def test_saved_explicit_empty_postprocessing_remains_disabled(self):
        for stage_type in (AIPrompt, AISystemOne, OpenAIDecisions):
            with self.subTest(stage=stage_type.__name__):
                with tempfile.TemporaryDirectory() as directory:
                    stage = stage_type().setPostProcessing("")
                    stage.write().save(directory + "/explicit")
                    loaded = stage_type.load(directory + "/explicit")
                    for candidate in (stage, loaded, loaded.copy({})):
                        with self.assertRaises(IllegalArgumentException):
                            candidate.setPostProcessingOptions({"delimiter": ";"})
                    implicit = stage_type()
                    implicit.write().save(directory + "/implicit")
                    restored = stage_type.load(directory + "/implicit")
                    restored.setPostProcessingOptions({"delimiter": ";"})
                    self.assertEqual(restored.getPostProcessing(), "csv")

    def test_existing_prompt_and_embedding_saved_stages_remain_directly_usable(self):
        cases = [
            (
                OpenAIPrompt,
                {
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "legacy answer",
                            },
                        }
                    ]
                },
            ),
            (OpenAIEmbedding, {"data": [{"index": 0, "embedding": [0.25, 0.5]}]}),
        ]
        for stage_type, reply in cases:
            with self.subTest(stage=stage_type.__name__), endpoint(reply) as (
                url,
                requests,
            ):
                stage = (
                    stage_type()
                    .setUrl(url + "/v1")
                    .setDeploymentName("existing-model")
                    .setOutputCol("answer")
                    .setErrorCol("error")
                )
                if stage_type is OpenAIPrompt:
                    stage.setPromptTemplate("{text}")
                else:
                    stage.setTextCol("text")
                with tempfile.TemporaryDirectory() as directory:
                    stage.write().save(directory + "/stage")
                    key = "spark.synapseml.legacy.allowUnsafeJavaDeserialization"
                    previous = spark.conf.get(key, None)
                    try:
                        # Only load the trusted legacy stage created by this test.
                        spark.conf.set(key, "true")
                        loaded = stage_type.load(directory + "/stage")
                    finally:
                        if previous is None:
                            spark.conf.unset(key)
                        else:
                            spark.conf.set(key, previous)
                    row = loaded.transform(
                        spark.createDataFrame([("existing input",)], "text STRING")
                    ).head()
                self.assertIsNone(row.error)
                if stage_type is OpenAIPrompt:
                    self.assertEqual(row.answer, "legacy answer")
                else:
                    self.assertEqual(row.answer.toArray().tolist(), [0.25, 0.5])
                self.assertEqual(len(requests), 1)
                self.assertEqual(requests[0][1]["model"], "existing-model")

    def test_generated_constructor_setparams_copy_and_load_reach_http(self):
        reply = {
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "answer"}}
            ],
            "future": {"retained": True},
        }
        with endpoint(reply) as (url, requests):
            original = AIPrompt(
                url=url + "/v1",
                deploymentName="test-model",
                inputCol="text",
                outputCol="answer",
                errorCol="error",
                rawResponseCol="raw",
                extraBody={"provider_option": {"enabled": True}},
            )
            extension = {
                "provider_option": [1, None, False, 9223372036854775808123456789]
            }
            original.setParams(extraBody=extension)
            with tempfile.TemporaryDirectory() as directory:
                original.copy({}).write().save(directory + "/stage")
                loaded = AIPrompt.load(directory + "/stage")
                self.assertEqual(loaded.getExtraBody(), extension)
                rows = loaded.transform(
                    spark.createDataFrame([("hello",)], "text STRING")
                ).collect()
            self.assertEqual(rows[0].answer, "answer")
            self.assertIsNone(rows[0].error)
            self.assertTrue(json.loads(rows[0].raw)["future"]["retained"])
            self.assertEqual(
                requests[0][1]["provider_option"], extension["provider_option"]
            )

    def test_native_dictionary_and_array_constructors_preserve_protocols(self):
        names = ["e_one", "d_two", "c_three", "b_four", "a_five", "z_six"]
        questions = {
            "urgent": {"type": "noul", "instructions": "Is this urgent?"},
            **{
                name: {
                    "type": "choice",
                    "instructions": name,
                    "criteria": {label: label for label in names},
                }
                for name in names
            },
        }
        cases = [
            (
                AISystemOne,
                "systemone",
                questions,
                {"model": "returned-model", "answers": {"urgent": {"result": True}}},
            ),
            (
                OpenAIDecisions,
                "decisions",
                [
                    {
                        "type": "predicate",
                        "name": "urgent",
                        "instructions": "Is this urgent?",
                    }
                ],
                {
                    "model": "returned-model",
                    "answers": [
                        {"type": "predicate", "name": "urgent", "probability": 0.9}
                    ],
                },
            ),
        ]
        for stage_type, api, questions, reply in cases:
            with self.subTest(api=api), endpoint(reply) as (url, requests):
                stage = stage_type(
                    url=url if api == "systemone" else url + "/v1",
                    model="explicit-model",
                    inputCol="text",
                    questions=questions,
                    outputCol="answer",
                    errorCol="error",
                )
                self.assertEqual(stage.getApiType(), api)
                with tempfile.TemporaryDirectory() as directory:
                    stage.copy({}).write().save(directory + "/native")
                    loaded = stage_type.load(directory + "/native")
                    self.assertEqual(loaded.getApiType(), api)
                    row = loaded.transform(
                        spark.createDataFrame([("urgent outage",)], "text STRING")
                    ).head()
                self.assertIsNone(row.error)
                self.assertEqual(requests[0][1]["questions"], questions)
                self.assertEqual(
                    json.dumps(requests[0][1]["questions"]), json.dumps(questions)
                )
                if api == "systemone":
                    self.assertTrue(json.loads(row.answer.answers["urgent"])["result"])
                else:
                    self.assertEqual(row.answer.answers[0].probability, 0.9)

    def test_body_json_validation_does_not_silently_coerce_values(self):
        cycle = {}
        cycle["self"] = cycle
        for value in ([1], {"x": float("nan")}, {1: "non-string key"}, cycle):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises((TypeError, ValueError)):
                    AIPrompt(extraBody=value)
                with self.assertRaises((TypeError, ValueError)):
                    AIPrompt().setParams(extraBody=value)
                with self.assertRaises((TypeError, ValueError)):
                    AIPrompt().setExtraBody(value)

    def test_column_binding_then_scalar_clears_stale_column(self):
        reply = {"data": [{"index": 0, "embedding": [0.25, 0.5]}]}
        with endpoint(reply) as (url, requests):
            stage = (
                AIPrompt()
                .setApiType("embeddings")
                .setUrl(url + "/v1")
                .setDeploymentName("embedding-model")
                .setInputCol("text")
                .setExtraBodyCol("absent")
                .setExtraBody({"encoding_format": "float"})
                .setUserCol("text")
                .setOutputCol("vector")
            )
            row = stage.transform(
                spark.createDataFrame([("hello",)], "text STRING")
            ).head()
            self.assertEqual(row.vector.toArray().tolist(), [0.25, 0.5])
            self.assertEqual(requests[0][1]["user"], "hello")
            self.assertEqual(requests[0][1]["encoding_format"], "float")


if __name__ == "__main__":
    unittest.main()
