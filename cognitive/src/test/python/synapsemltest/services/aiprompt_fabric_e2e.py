# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Opt-in managed Spark qualification using synthetic native HTTP replies.

Supply candidate core, cognitive and cognitive test JARs to Fabric Spark using
--expected-core-jar, --expected-cognitive-jar and --expected-tests-jar.
Each expected name must exactly match the URL-decoded loaded JAR basename.
Run without Python optimization so qualification assertions remain enabled.
This script does not provision storage, acquire model credentials, or claim
live-provider qualification.
"""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

from pyspark.sql import DataFrame, SparkSession


def main():
    if not __debug__:
        raise RuntimeError(
            "Fabric qualification requires Python assertions; disable -O and PYTHONOPTIMIZE"
        )
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-core-jar", required=True)
    parser.add_argument("--expected-cognitive-jar", required=True)
    parser.add_argument("--expected-tests-jar", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    jvm = spark._jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()

    def source(name):
        cls = jvm.java.lang.Class.forName(name, True, loader)
        return str(cls.getProtectionDomain().getCodeSource().getLocation().toString())

    provenance = {
        "core": source("org.apache.spark.ml.ExplicitComplexParamsWritable"),
        "cognitive": source("com.microsoft.azure.synapse.ml.services.ai.AIPrompt"),
        "basePrompt": source(
            "com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt"
        ),
        "probe": source("com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricProbe"),
    }
    for role, expected in (
        ("core", args.expected_core_jar),
        ("cognitive", args.expected_cognitive_jar),
        ("basePrompt", args.expected_cognitive_jar),
        ("probe", args.expected_tests_jar),
    ):
        basename = unquote(urlsplit(provenance[role]).path.rsplit("/", 1)[-1])
        assert expected and basename == expected, provenance
    implicit_apis = json.loads(
        jvm.com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricProbe.verifyImplicitEndpoints(
            spark._jsparkSession
        )
    )
    assert implicit_apis == ["chat_completions", "responses", "embeddings"]
    sends = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            request = json.loads(
                self.rfile.read(int(self.headers["Content-Length"])).decode("utf-8")
            )
            with lock:
                sends.append(request["state"])
            assert self.path == "/providers/microsoft/v1/systemone"
            body = json.dumps(
                {
                    "model": "synthetic-systemone",
                    "answers": {"urgent": True, "team": "engineering", "severity": 2},
                    "usage": {"input_tokens": 3, "output_tokens": 0, "total_tokens": 3},
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    questions = {
        "urgent": {"type": "noul", "instructions": "Is this urgent?"},
        "team": {
            "type": "choice",
            "instructions": "Choose a team.",
            "criteria": {"billing": "Billing", "engineering": "Outages"},
        },
        "severity": {
            "type": "score",
            "instructions": "Rate severity.",
            "criteria": ["Cosmetic", "Major", "Critical"],
        },
    }
    host = spark.conf.get("spark.driver.host")
    server = ThreadingHTTPServer((host, 0), Handler)
    endpoint = f"http://{host}:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        stage = (
            jvm.com.microsoft.azure.synapse.ml.services.ai.AISystemOne()
            .setUrl(endpoint)
            .setModel("synthetic-model")
            .setStateCol("text")
            .setQuestions(json.dumps(questions))
            .setOutputCol("answer")
            .setErrorCol("error")
            .setUsageCol("usage")
            .setRawResponseCol("raw")
            .setConcurrency(2)
        )
        data = spark.createDataFrame(
            [(1, "outage"), (2, "outage"), (3, None), (4, "billing")],
            "id INT, text STRING",
        ).repartition(2)
        expected = stage.transformSchema(data._jdf.schema()).json()
        result = DataFrame(stage.transform(data._jdf), spark)
        assert sends == [], "Transform must remain lazy"
        assert json.loads(result.schema.json()) == json.loads(expected)
        rows = result.collect()
        assert len(rows) == 4 and len(sends) == 3
        assert sum(row.error is not None for row in rows) == 1
        assert sum(row.text == "outage" for row in rows) == 2
        assert all(
            set(row.answer.answers) == set(questions)
            for row in rows
            if row.error is None
        )
        evidence = {
            "applicationId": spark.sparkContext.applicationId,
            "sparkVersion": spark.version,
            "classSources": provenance,
            "rows": len(rows),
            "physicalSends": len(sends),
            "implicitApis": implicit_apis,
            "synthetic": True,
        }
        print(json.dumps(evidence, sort_keys=True))
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
