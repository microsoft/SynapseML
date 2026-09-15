# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Public-API Fabric regression for #2702 and #2703, submitted with fabric-spark-cli."""

import argparse
import hashlib
import json
import os
import socket
import time
import uuid
from pathlib import Path

from pyspark.ml.functions import vector_to_array
from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.sql import SparkSession, functions as sf
from synapse.ml.lightgbm import (
    LightGBMClassifier,
    LightGBMRanker,
    LightGBMRegressionModel,
    LightGBMRegressor,
)


def class_source(spark, name):
    jvm = spark.sparkContext._jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    cls = jvm.java.lang.Class.forName(name, True, loader)
    return str(cls.getProtectionDomain().getCodeSource().getLocation().toString())


def native_files(pid):
    paths = set()
    for line in Path(f"/proc/{pid}/maps").read_text().splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) == 6 and "lib_lightgbm" in fields[5]:
            paths.add(fields[5])
    return {
        Path(path).name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in sorted(paths)
    }


def inspect_executor(batches):
    import pandas as pd

    for batch in batches:
        # Python workers descend from the executor JVM through its Python daemon.
        pid = os.getpid()
        while pid > 1:
            command = Path(f"/proc/{pid}/cmdline").read_bytes()
            if b"CoarseGrainedExecutorBackend" in command:
                break
            status = Path(f"/proc/{pid}/status").read_text()
            pid = int(
                next(
                    line.split()[1]
                    for line in status.splitlines()
                    if line.startswith("PPid:")
                )
            )
        if pid <= 1:
            raise RuntimeError("Could not locate the Spark executor JVM")
        status = Path(f"/proc/{pid}/status").read_text().splitlines()
        rss = int(next(line.split()[1] for line in status if line.startswith("VmRSS:")))
        time.sleep(0.2)
        yield pd.DataFrame(
            [(socket.gethostname(), pid, rss, json.dumps(native_files(pid)))],
            columns=["host", "pid", "rssKiB", "native"],
        )


def executor_snapshot(spark, partitions):
    return (
        spark.range(partitions, numPartitions=partitions)
        .mapInPandas(
            inspect_executor, "host string, pid long, rssKiB long, native string"
        )
        .collect()
    )


def run(args):
    if not __debug__:
        raise RuntimeError("This regression requires assertions")
    spark = SparkSession.builder.getOrCreate()
    sources = {
        "core": class_source(spark, "com.microsoft.azure.synapse.ml.build.BuildInfo$"),
        "lightgbm": class_source(
            spark, "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressor"
        ),
        "native": class_source(spark, "com.microsoft.ml.lightgbm.lightgbmlib"),
    }
    for key, expected in (
        ("core", args.expected_core_jar),
        ("lightgbm", args.expected_lightgbm_jar),
        ("native", args.expected_native_jar),
    ):
        assert expected in sources[key], (expected, sources[key])
    jvm = spark.sparkContext._jvm
    utils = getattr(jvm.com.microsoft.azure.synapse.ml.lightgbm, "LightGBMUtils$")
    getattr(utils, "MODULE$").initializeNativeLibrary()
    driver_native = native_files(int(jvm.java.lang.ProcessHandle.current().pid()))
    assert len(driver_native) == 2, driver_native
    assert sorted(driver_native.values()) == sorted(args.native_sha256), driver_native
    diagnostics = {
        "applicationId": spark.sparkContext.applicationId,
        "sparkVersion": spark.version,
        "classSources": sources,
        "driverNativeHashes": driver_native,
    }
    print("SYNAPSEML_FABRIC_E2E_DIAGNOSTIC=" + json.dumps(diagnostics, sort_keys=True))
    deadline = time.monotonic() + 120
    while True:
        before = executor_snapshot(spark, args.partitions)
        executors = {(row.host, row.pid) for row in before}
        if len(executors) == args.executors:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Expected {args.executors} executor JVMs, observed {executors}"
            )
        time.sleep(2)

    last_id = args.rows - 1

    def dataset(sparse, invalid_size=None, validation=False):
        @sf.udf(returnType=VectorUDT())
        def features(index):
            size = invalid_size if index == last_id and invalid_size is not None else 1
            value = float(index % 17 + 1) if index % 8 == 0 else 0.0
            values = [value] * size
            return (
                Vectors.sparse(
                    size, {i: value for i, value in enumerate(values) if value != 0}
                )
                if sparse
                else Vectors.dense(values)
            )

        return spark.range(args.rows, numPartitions=args.partitions).select(
            "id",
            (sf.col("id") % 2).cast("double").alias("label"),
            (sf.col("id") / 32).cast("int").alias("group"),
            (
                sf.lit(validation)
                & ((sf.col("id") % 5 == 0) | (sf.col("id") == last_id))
            ).alias("validation"),
            features("id").alias("features"),
        )

    def learner(kind, matrix, validation=False):
        cls = {
            "regressor": LightGBMRegressor,
            "classifier": LightGBMClassifier,
            "ranker": LightGBMRanker,
        }[kind]
        result = cls(
            dataTransferMode="streaming",
            useSingleDatasetMode=True,
            numTasks=args.partitions,
            numThreads=1,
            maxStreamingOMPThreads=1,
            microBatchSize=8,
            matrixType=matrix,
            samplingMode="fixed",
            binSampleCount=16,
            minDataPerBin=1,
            minDataInLeaf=1,
            numIterations=5,
            numLeaves=4,
            verbosity=2,
            timeout=60,
            deterministic=True,
            seed=731,
            passThroughArgs="force_col_wise=true is_enable_sparse=true sparse_threshold=0.1",
        )
        if validation:
            result.setValidationIndicatorCol("validation")
        if kind == "ranker":
            result.setGroupCol("group")
        return result

    outcomes = []
    for matrix in ("dense", "sparse"):
        for validation in (False, True):
            frame = dataset(
                matrix == "sparse", invalid_size=0, validation=validation
            ).cache()
            try:
                count = frame.count()
                fingerprint = (
                    frame.withColumn("values", vector_to_array("features"))
                    .select(
                        sf.expr(
                            "bit_xor(xxhash64(id, label, validation, values))"
                        ).alias("fingerprint")
                    )
                    .first()[0]
                )
                assert count == args.rows
                assert (
                    frame.where(sf.size(vector_to_array("features")) == 0).count() == 1
                )
                started = time.monotonic()
                try:
                    learner("regressor", matrix, validation).fit(frame)
                except Exception as error:
                    if (
                        args.expect_malformed != "reject"
                        or "Expected feature vector size 1 but found 0"
                        not in str(error)
                    ):
                        raise
                    outcome = "rejected"
                else:
                    if args.expect_malformed != "accept":
                        raise AssertionError("fit accepted a malformed feature vector")
                    outcome = "accepted"
                outcomes.append(
                    {
                        "matrix": matrix,
                        "validation": validation,
                        "malformed": True,
                        "outcome": outcome,
                        "seconds": time.monotonic() - started,
                        "rows": count,
                        "fingerprint": fingerprint,
                    }
                )
            finally:
                frame.unpersist()

        frame = dataset(matrix == "sparse", validation=True).cache()
        try:
            assert frame.count() == args.rows
            for repetition in range(args.repetitions):
                started = time.monotonic()
                estimator = learner("regressor", matrix, validation=True)
                model = estimator.fit(frame)
                fit_seconds = time.monotonic() - started
                measures = estimator._java_obj.getPerformanceMeasures().get()
                task_measures = measures.getTaskMeasures()
                timings = [
                    {
                        "partition": task_measures.apply(i).partitionId(),
                        "active": task_measures.apply(i).isActiveTrainingTask(),
                        "preparationMs": task_measures.apply(i).dataPreparationTime(),
                        "finalizationMs": task_measures.apply(i).datasetCreationTime(),
                    }
                    for i in range(task_measures.size())
                ]
                predictions = model.transform(frame).select("id", "prediction").cache()
                path = f"Files/lightgbm-regression-{uuid.uuid4().hex}"
                try:
                    assert predictions.count() == args.rows
                    assert (
                        predictions.where(
                            sf.col("prediction").isNull()
                            | sf.isnan("prediction")
                            | (sf.abs("prediction") == float("inf"))
                        ).count()
                        == 0
                    )
                    mean = predictions.agg(sf.avg("prediction")).first()[0]
                    model.write().overwrite().save(path)
                    loaded = LightGBMRegressionModel.load(path)
                    reloaded = loaded.transform(frame).select("id", "prediction")
                    assert predictions.exceptAll(reloaded).limit(1).count() == 0
                    assert reloaded.exceptAll(predictions).limit(1).count() == 0
                    outcomes.append(
                        {
                            "matrix": matrix,
                            "malformed": False,
                            "repetition": repetition,
                            "rows": args.rows,
                            "predictionMean": mean,
                            "saveLoadEqual": True,
                            "fitSeconds": fit_seconds,
                            "rowCountMs": measures.rowCountTime(),
                            "taskTimings": timings,
                            "seconds": time.monotonic() - started,
                        }
                    )
                finally:
                    predictions.unpersist()
                    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
                    fs = hadoop_path.getFileSystem(
                        spark.sparkContext._jsc.hadoopConfiguration()
                    )
                    if fs.exists(hadoop_path):
                        assert fs.delete(hadoop_path, True)
        finally:
            frame.unpersist()
        print(
            "SYNAPSEML_FABRIC_E2E_DIAGNOSTIC="
            + json.dumps({"phase": matrix, "outcomes": outcomes})
        )

    if args.expect_malformed == "reject":
        frame = dataset(sparse=True).cache()
        try:
            assert frame.count() == args.rows
            for kind in ("classifier", "ranker"):
                model = learner(kind, "sparse").fit(frame)
                predictions = model.transform(frame).select("prediction")
                assert predictions.count() == args.rows
                assert (
                    predictions.where(
                        sf.isnan("prediction") | sf.col("prediction").isNull()
                    ).count()
                    == 0
                )
                outcomes.append(
                    {"learner": kind, "rows": args.rows, "outcome": "passed"}
                )
        finally:
            frame.unpersist()

    after = executor_snapshot(spark, args.partitions)
    assert {
        (row.host, row.pid) for row in after
    } == executors, "Executor membership changed"
    for row in after:
        hashes = json.loads(row.native)
        assert len(hashes) == 2 and sorted(hashes.values()) == sorted(
            args.native_sha256
        ), row
    print(
        "SYNAPSEML_FABRIC_E2E_RESULT="
        + json.dumps(
            {
                **diagnostics,
                "outcomes": outcomes,
                "expectedExecutors": args.executors,
                "executorSamplesBefore": [row.asDict() for row in before],
                "executorSamples": [row.asDict() for row in after],
                "partitions": args.partitions,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-core-jar", required=True)
    parser.add_argument("--expected-lightgbm-jar", required=True)
    parser.add_argument("--expected-native-jar", required=True)
    parser.add_argument("--native-sha256", action="append", required=True)
    parser.add_argument(
        "--expect-malformed", choices=("accept", "reject"), default="reject"
    )
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--executors", type=int, default=2)
    parser.add_argument("--repetitions", type=int, default=2)
    run(parser.parse_args())
