# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Assert that Fabric loaded an explicitly supplied SynapseML jar."""

if not __debug__:
    raise RuntimeError("Fabric E2E scenarios require Python assertions")

import argparse
import json

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--class-name", required=True)
parser.add_argument("--expected-jar-token", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
loader = (
    spark.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader()
)
loaded_class = spark.sparkContext._jvm.java.lang.Class.forName(
    args.class_name, True, loader
)
code_source = str(
    loaded_class.getProtectionDomain().getCodeSource().getLocation().toString()
)
assert args.expected_jar_token.lower() in code_source.lower(), (
    f"Expected {args.class_name} to load from a jar containing "
    f"{args.expected_jar_token!r}, got {code_source!r}"
)

count = spark.range(128).repartition(4).where("id % 4 = 0").count()
assert count == 32, f"Expected 32 rows, got {count}"

evidence = {
    "applicationId": spark.sparkContext.applicationId,
    "className": args.class_name,
    "classSource": code_source,
    "count": count,
    "defaultParallelism": spark.sparkContext.defaultParallelism,
    "sparkVersion": spark.version,
}
print("SYNAPSEML_FABRIC_E2E_RESULT=" + json.dumps(evidence, sort_keys=True))
