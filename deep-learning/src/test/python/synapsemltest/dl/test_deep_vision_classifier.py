# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import contextlib
import os
import shutil
import tempfile

import pytest
from horovod.spark.common.store import LocalStore
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pytorch_lightning.callbacks import ModelCheckpoint
from synapse.ml.dl import *

from .conftest import CallbackBackend
from .test_deep_vision_model import MyDummyCallback


@contextlib.contextmanager
def tempdir():
    dirpath = tempfile.mkdtemp()
    try:
        yield dirpath
    finally:
        shutil.rmtree(dirpath)


@contextlib.contextmanager
def local_store():
    with tempdir() as tmp:
        store = LocalStore(tmp)
        yield store


def generate_data(spark, train_folder, test_folder):
    train_files = [
        os.path.join(dp, f)
        for dp, dn, filenames in os.walk(train_folder)
        for f in filenames
        if os.path.splitext(f)[1] == ".jpg"
    ]
    test_files = [
        os.path.join(dp, f)
        for dp, dn, filenames in os.walk(test_folder)
        for f in filenames
        if os.path.splitext(f)[1] == ".jpg"
    ]

    def extract_path_and_label(path):
        num = int(path.split("/")[-1].split(".")[0].split("_")[1])
        label = num // 81  # Assign the label
        return (path, label)

    train_df = spark.createDataFrame(
        map(extract_path_and_label, train_files), ["image", "label"]
    ).withColumn("label", col("label").cast(DoubleType()))

    test_df = spark.createDataFrame(
        map(extract_path_and_label, test_files), ["image", "label"]
    ).withColumn("label", col("label").cast(DoubleType()))

    return train_df, test_df


@pytest.mark.skip(reason="not testing this for now")
def test_mobilenet_v2(get_data_path):
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    ctx = CallbackBackend()

    epochs = 5
    callbacks = [
        MyDummyCallback(epochs),
        ModelCheckpoint(dirpath="target/mobilenet_v2/"),
    ]

    train_folder, test_folder = get_data_path

    with local_store() as store:
        deep_vision_classifier = DeepVisionClassifier(
            backbone="mobilenet_v2",
            store=store,
            backend=ctx,
            callbacks=callbacks,
            num_classes=17,
            batch_size=16,
            epochs=epochs,
            validation=0.1,
        )

        train_df, test_df = generate_data(spark, train_folder, test_folder)

        deep_vision_model = deep_vision_classifier.fit(train_df)

        pred_df = deep_vision_model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            predictionCol="prediction", labelCol="label", metricName="accuracy"
        )
        accuracy = evaluator.evaluate(pred_df)
        assert accuracy > 0.5

    spark.stop()
