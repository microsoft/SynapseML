# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Exercise repeated LightGBM streaming fits on managed Fabric Spark."""

import argparse
import json
from pathlib import Path

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions as sf
from synapse.ml.lightgbm import LightGBMClassifier


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


def block_manager_addresses(spark_session):
    iterator = (
        spark_session.sparkContext._jsc.sc().getExecutorMemoryStatus().keysIterator()
    )
    addresses = []
    while iterator.hasNext():
        addresses.append(str(iterator.next()))
    return sorted(addresses)


def mapped_lightgbm_libraries(maps_text):
    """Return mapped LightGBM paths, preserving optional deleted suffixes."""
    paths = set()
    for line in maps_text.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) == 6 and "lightgbm" in fields[5].lower():
            paths.add(fields[5])
    return sorted(paths)


def native_diagnostics(spark_session, class_sources):
    jvm = spark_session.sparkContext._jvm
    lightgbm_utils = getattr(
        jvm.com.microsoft.azure.synapse.ml.lightgbm, "LightGBMUtils$"
    )
    getattr(lightgbm_utils, "MODULE$").initializeNativeLibrary()
    driver_pid = int(jvm.java.lang.ProcessHandle.current().pid())
    native_mappings = mapped_lightgbm_libraries(
        Path(f"/proc/{driver_pid}/maps").read_text()
    )
    return {
        **class_sources,
        "applicationId": spark_session.sparkContext.applicationId,
        "defaultParallelism": spark_session.sparkContext.defaultParallelism,
        "driverJavaLibraryPath": str(
            jvm.java.lang.System.getProperty("java.library.path")
        ),
        "driverJvmPid": driver_pid,
        "driverNativeMappings": native_mappings,
        "phase": "native-load",
        "sparkVersion": spark_session.version,
    }


parser = argparse.ArgumentParser()
parser.add_argument("--expected-core-jar", required=True)
parser.add_argument("--expected-lightgbm-jar", required=True)
parser.add_argument("--expected-native-jar", required=True)
parser.add_argument("--native-threads", type=int, default=2)
parser.add_argument("--partitions", type=int, default=4)
parser.add_argument("--repetitions", type=int, default=2)
parser.add_argument("--rows", type=int, default=4000)
args = parser.parse_args()

if args.native_threads < 1 or args.partitions < 2:
    raise ValueError(
        "native-threads must be positive and partitions must be at least 2"
    )
if args.repetitions < 1 or args.rows < 100:
    raise ValueError("repetitions must be positive and rows must be at least 100")

spark = SparkSession.builder.getOrCreate()
core_source = class_source(spark, "com.microsoft.azure.synapse.ml.build.BuildInfo$")
lightgbm_source = class_source(
    spark, "com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassifier"
)
native_source = class_source(spark, "com.microsoft.ml.lightgbm.lightgbmlib")
assert (
    args.expected_core_jar.lower() in core_source.lower()
), f"Expected the PR core jar {args.expected_core_jar!r}, got {core_source!r}"
assert args.expected_native_jar.lower() in native_source.lower(), (
    f"Expected the LightGBM JNI jar {args.expected_native_jar!r}, "
    f"got {native_source!r}"
)
assert args.expected_lightgbm_jar.lower() in lightgbm_source.lower(), (
    f"Expected the PR LightGBM jar {args.expected_lightgbm_jar!r}, "
    f"got {lightgbm_source!r}"
)
diagnostics = native_diagnostics(
    spark,
    {
        "coreClassSource": core_source,
        "lightgbmClassSource": lightgbm_source,
        "nativeClassSource": native_source,
    },
)
print("SYNAPSEML_FABRIC_E2E_DIAGNOSTIC=" + json.dumps(diagnostics, sort_keys=True))

# Keep feature count at one so this lifecycle test is not masked by the
# separate multi-feature reference-dataset naming defect.
raw = spark.range(args.rows).select(
    (sf.col("id") % 2).cast("double").alias("label"),
    ((sf.col("id") % 17) / 17.0).cast("double").alias("feature_1"),
    (sf.col("id") % 5 == 0).alias("is_validation"),
)
dataset = (
    VectorAssembler(inputCols=["feature_1"], outputCol="features")
    .transform(raw)
    .select("label", "features", "is_validation")
    .repartition(args.partitions)
    .cache()
)
prediction_counts = []
try:
    assert dataset.count() == args.rows

    learner = LightGBMClassifier(
        dataTransferMode="streaming",
        featuresCol="features",
        labelCol="label",
        numIterations=5,
        numLeaves=8,
        numTasks=args.partitions,
        numThreads=args.native_threads,
        slotNames=["feature_one"],
        useSingleDatasetMode=True,
        validationIndicatorCol="is_validation",
        verbosity=-1,
    )

    for repetition in range(args.repetitions):
        try:
            model = learner.fit(dataset)
        except Exception as error:
            error_lines = [
                line.strip() for line in str(error).splitlines() if line.strip()
            ]
            native_error = next(
                (
                    line
                    for line in reversed(error_lines)
                    if "call failed in LightGBM" in line
                ),
                error_lines[-1] if error_lines else repr(error),
            )
            print(
                "SYNAPSEML_FABRIC_E2E_DIAGNOSTIC="
                + json.dumps(
                    {
                        "errorMessage": native_error,
                        "errorType": type(error).__name__,
                        "fitAttempt": repetition + 1,
                        "phase": "fit",
                    },
                    sort_keys=True,
                )
            )
            raise
        prediction_count = model.transform(dataset).select("prediction").count()
        assert prediction_count == args.rows
        prediction_counts.append(prediction_count)
finally:
    dataset.unpersist()

evidence = {
    **diagnostics,
    "blockManagerAddresses": block_manager_addresses(spark),
    "executorCores": spark.conf.get("spark.executor.cores", "unknown"),
    "partitions": args.partitions,
    "predictionCounts": prediction_counts,
    "repetitions": args.repetitions,
    "rows": args.rows,
    "sparkVersion": spark.version,
}
print("SYNAPSEML_FABRIC_E2E_RESULT=" + json.dumps(evidence, sort_keys=True))
