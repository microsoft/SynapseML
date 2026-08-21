# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Minimal managed Fabric Spark execution proof."""

import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
count = spark.range(32).repartition(4).where("id % 2 = 0").count()
assert count == 16, f"Expected 16 even values, got {count}"

evidence = {
    "applicationId": spark.sparkContext.applicationId,
    "count": count,
    "defaultParallelism": spark.sparkContext.defaultParallelism,
    "sparkVersion": spark.version,
}
print("SYNAPSEML_FABRIC_E2E_RESULT=" + json.dumps(evidence, sort_keys=True))
