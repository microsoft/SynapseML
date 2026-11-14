# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Opt-in same-application recovery probe using synthetic native HTTP replies.

This script does not qualify recovery across managed Spark application restarts.

Supply the PR core, cognitive and cognitive test JARs to Fabric Spark, using
--expected-core-jar, --expected-cognitive-jar and --expected-tests-jar respectively.
Each expected name must exactly match the URL-decoded loaded JAR basename.
Run without Python optimization so the qualification assertions remain enabled.
The runner owns the scratch Lakehouse lifecycle. This script does not provision
resources, acquire model credentials, or claim live-provider qualification.
"""

import argparse
import json
import threading
import uuid
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
        "recovery": source(
            "com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricRecovery"
        ),
        "probe": source(
            "com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricProbe"
        ),
    }
    for role, expected in (
        ("core", args.expected_core_jar),
        ("cognitive", args.expected_cognitive_jar),
        ("basePrompt", args.expected_cognitive_jar),
        ("recovery", args.expected_tests_jar),
        ("probe", args.expected_tests_jar),
    ):
        basename = unquote(urlsplit(provenance[role]).path.rsplit("/", 1)[-1])
        assert expected and basename == expected, provenance
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

    relative = "Files/aiprompt_e2e/" + uuid.uuid4().hex
    hadoop_path = jvm.org.apache.hadoop.fs.Path(relative)
    filesystem = hadoop_path.getFileSystem(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    qualified = str(filesystem.makeQualified(hadoop_path))
    assert qualified.startswith(("abfs:", "abfss:")), (
        "This scenario must exercise OneLake/ABFS, not local storage",
        qualified,
    )
    jvm.com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricRecovery.verifyLocalStorageRejected(
        spark._jsparkSession
    )
    implicit_apis = json.loads(
        jvm.com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricProbe.verifyImplicitEndpoints(
            spark._jsparkSession
        )
    )
    assert implicit_apis == ["chat_completions", "responses", "embeddings"]
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
    companion = jvm.com.microsoft.azure.synapse.ml.services.ai.AIPrompt

    def configured():
        return (
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
            .setOutputPath(qualified)
            .setOutputBatchSize(2)
        )

    host = spark.conf.get("spark.driver.host")
    server = ThreadingHTTPServer((host, 0), Handler)
    endpoint = f"http://{host}:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        data = spark.createDataFrame(
            [(1, "outage"), (2, "outage"), (3, None), (4, "billing")],
            "id INT, text STRING",
        ).repartition(2)
        stage = configured()
        expected = stage.transformSchema(data._jdf.schema()).json()
        jvm.com.microsoft.azure.synapse.ml.services.ai.AIPromptFabricRecovery.stopAfterFirstBatch(
            stage, data._jdf
        )
        partial = json.loads(
            companion.getOutputStatusJson(spark._jsparkSession, qualified)
        )
        assert partial["committedCount"] == 2 and partial["pendingCount"] == 2
        assert partial["ownerToken"] is not None
        committed = DataFrame(
            companion.loadOutput(spark._jsparkSession, qualified), spark
        ).collect()
        before_resume = len(sends)
        assert len(committed) == 2
        companion.releaseOutputOwner(
            spark._jsparkSession, qualified, partial["ownerToken"], True
        )
        result = DataFrame(configured().resume(spark._jsparkSession), spark)
        assert json.loads(result.schema.json()) == json.loads(expected), (
            expected,
            result.schema.json(),
        )
        assert result.count() == 4
        assert len(sends) == 3
        rows = result.collect()
        assert sum(row.error is not None for row in rows) == 1
        assert sum(row.text == "outage" for row in rows) == 2
        assert all(
            set(row.answer.answers) == set(questions)
            for row in rows
            if row.error is None
        )
        status = json.loads(
            companion.getOutputStatusJson(spark._jsparkSession, qualified)
        )
        reopened = DataFrame(
            companion.loadOutput(spark._jsparkSession, qualified), spark
        )
        assert reopened.schema == result.schema
        assert reopened.count() == 4
        resumed = DataFrame(configured().resume(spark._jsparkSession), spark)
        assert resumed.schema == result.schema and resumed.count() == 4
        assert len(sends) == 3, "Reading/resuming committed rows must not send HTTP"
        evidence = {
            "applicationId": spark.sparkContext.applicationId,
            "sparkVersion": spark.version,
            "classSources": provenance,
            "storageScheme": qualified.split(":", 1)[0],
            "rows": len(rows),
            "physicalSends": len(sends),
            "committedBeforeRecovery": partial["committedCount"],
            "sendsBeforeRecovery": before_resume,
            "matchingOwnerRecovery": True,
            "status": status,
            "schemaEqual": True,
            "duplicateRowsRetained": True,
            "credentialFreeMock": True,
            "implicitFabricApis": implicit_apis,
            "nonLocalFileStorageRejected": True,
        }
        print("SYNAPSEML_FABRIC_E2E_RESULT=" + json.dumps(evidence, sort_keys=True))
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
