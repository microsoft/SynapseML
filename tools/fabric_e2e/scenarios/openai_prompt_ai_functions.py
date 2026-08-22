# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Exercise OpenAIPrompt behaviors inspired by PySpark AI Functions."""

if not __debug__:
    raise RuntimeError("Fabric E2E scenarios require Python assertions")

import argparse
import inspect
import json

from pyspark.sql import SparkSession
from synapse.ml.services.openai import OpenAIPrompt


def class_source(spark_session, class_name):
    loader = (
        spark_session.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    )
    loaded_class = spark_session.sparkContext._jvm.java.lang.Class.forName(
        class_name, True, loader
    )
    return str(
        loaded_class.getProtectionDomain().getCodeSource().getLocation().toString()
    )


def emit_diagnostic(payload):
    print("SYNAPSEML_FABRIC_E2E_DIAGNOSTIC=" + json.dumps(payload, sort_keys=True))


parser = argparse.ArgumentParser()
parser.add_argument("--expected-cognitive-jar", required=True)
parser.add_argument("--expected-core-jar", required=True)
parser.add_argument("--model", default="gpt-5-mini")
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
core_source = class_source(spark, "com.microsoft.azure.synapse.ml.fabric.FabricClient$")
cognitive_source = class_source(
    spark, "com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt"
)
responses_source = class_source(
    spark, "com.microsoft.azure.synapse.ml.services.openai.OpenAIResponses"
)
assert (
    args.expected_core_jar.lower() in core_source.lower()
), f"Expected the PR core jar {args.expected_core_jar!r}, got {core_source!r}"
assert args.expected_cognitive_jar.lower() in cognitive_source.lower(), (
    f"Expected the PR cognitive jar {args.expected_cognitive_jar!r}, "
    f"got {cognitive_source!r}"
)
assert args.expected_cognitive_jar.lower() in responses_source.lower(), (
    f"Expected OpenAIResponses in {args.expected_cognitive_jar!r}, "
    f"got {responses_source!r}"
)

prompt = (
    OpenAIPrompt()
    .setApiType("responses")
    .setApiVersion("2025-04-01-preview")
    .setConcurrency(1)
    .setDeploymentName(args.model)
    .setErrorCol("service_error")
    .setOutputCol("analysis")
    .setPostProcessing("json")
    .setPostProcessingOptions(
        {"jsonSchema": "sentiment STRING, summary STRING, spanish STRING"}
    )
    .setPromptTemplate(
        """Analyze the review below and return only one JSON object.
The object must contain exactly these string fields:
- sentiment: exactly one of positive, negative, or neutral
- summary: an English summary of at most eight words
- spanish: a faithful Spanish translation of the review
Review: {review}"""
    )
    .setResponseFormat("json_object")
    .setSystemPrompt(
        "Follow the requested JSON schema exactly. Do not add markdown or commentary."
    )
    .setUsageCol("usage")
)

java_prompt = prompt._java_obj
fabric_client = getattr(
    spark.sparkContext._jvm.com.microsoft.azure.synapse.ml.fabric, "FabricClient$"
)
fabric_endpoint = str(getattr(fabric_client, "MODULE$").MLWorkloadEndpointOpenAI())
explicit_credentials = {
    name: bool(java_prompt.isSet(java_prompt.getParam(name)))
    for name in ("AADToken", "CustomAuthHeader", "customHeaders", "subscriptionKey")
    if java_prompt.hasParam(name)
}
explicit_endpoint = bool(java_prompt.isSet(java_prompt.getParam("url")))
assert not any(
    explicit_credentials.values()
), "The Fabric scenario must not set an explicit OpenAI credential"
assert (
    not explicit_endpoint
), "The Fabric scenario must not set an explicit OpenAI endpoint"
assert (
    "/cognitive/openai/" in fabric_endpoint.lower()
), "Fabric did not expose its implicit OpenAI workload endpoint"

base_diagnostics = {
    "apiType": "responses",
    "applicationId": spark.sparkContext.applicationId,
    "cognitiveClassSource": cognitive_source,
    "coreClassSource": core_source,
    "explicitCredentials": explicit_credentials,
    "explicitEndpoint": explicit_endpoint,
    "implicitFabricEndpoint": True,
    "model": args.model,
    "phase": "provenance",
    "pythonWrapperSource": inspect.getfile(OpenAIPrompt),
    "responsesClassSource": responses_source,
    "sparkVersion": spark.version,
}
emit_diagnostic(base_diagnostics)

cases = [
    (
        "positive",
        "I love this product. It is excellent and works perfectly.",
        "positive",
    ),
    (
        "negative",
        "This is terrible. It broke immediately and I hate it.",
        "negative",
    ),
    (
        "neutral",
        "The package arrived on Tuesday and contains a black cable.",
        "neutral",
    ),
    ("null", None, None),
]
dataset = spark.createDataFrame(
    cases, "case_id STRING, review STRING, expected_sentiment STRING"
)

try:
    result = prompt.transform(dataset)
    expected_schema = "struct<sentiment:string,summary:string,spanish:string>"
    actual_schema = result.schema["analysis"].dataType.simpleString()
    assert (
        actual_schema == expected_schema
    ), f"Expected analysis schema {expected_schema}, got {actual_schema}"

    rows = result.select(
        "case_id",
        "expected_sentiment",
        "analysis",
        "usage",
        "service_error",
    ).collect()
    assert len(rows) == len(cases), f"Expected {len(cases)} rows, got {len(rows)}"

    sentiments = {}
    token_totals = {}
    for row in rows:
        case_id = row["case_id"]
        if case_id == "null":
            assert row["analysis"] is None, "Null input must produce null output"
            assert row["usage"] is None, "Null input must not report model usage"
            assert (
                row["service_error"] is None
            ), "Null input must not issue a failing service request"
            continue

        assert (
            row["service_error"] is None
        ), f"Case {case_id!r} returned a service error"
        output = row["analysis"]
        assert output is not None, f"Case {case_id!r} returned no structured output"
        values = output.asDict()
        sentiment = (values.get("sentiment") or "").strip().lower()
        summary = (values.get("summary") or "").strip()
        spanish = (values.get("spanish") or "").strip()
        assert sentiment == row["expected_sentiment"], (
            f"Case {case_id!r} expected sentiment "
            f"{row['expected_sentiment']!r}, got {sentiment!r}"
        )
        assert summary, f"Case {case_id!r} returned an empty summary"
        assert spanish, f"Case {case_id!r} returned an empty translation"
        sentiments[case_id] = sentiment

        usage = row["usage"]
        assert usage is not None, f"Case {case_id!r} returned no usage data"
        total_tokens = usage["total_tokens"]
        assert total_tokens is None or total_tokens >= 0
        token_totals[case_id] = total_tokens
except Exception as error:
    emit_diagnostic(
        {
            "errorType": type(error).__name__,
            "phase": "transform-and-assert",
        }
    )
    raise

evidence = {
    **base_diagnostics,
    "analysisSchema": actual_schema,
    "caseCount": len(rows),
    "nullPropagation": True,
    "phase": "complete",
    "sentiments": sentiments,
    "testCases": [
        "generate_response",
        "analyze_sentiment",
        "summarize",
        "translate",
        "structured_extraction",
        "null_propagation",
    ],
    "tokenTotals": token_totals,
}
print("SYNAPSEML_FABRIC_E2E_RESULT=" + json.dumps(evidence, sort_keys=True))
