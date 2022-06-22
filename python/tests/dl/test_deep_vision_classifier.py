# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import contextlib
import os
import shutil
import sys
import tempfile

import numpy as np
from horovod.spark.common.backend import SparkBackend
from horovod.spark.common.store import LocalStore
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from pytorch_lightning.callbacks import ModelCheckpoint

from .test_deep_vision_model import MyDummyCallback

sys.path.append(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "synapse", "ml", "dl")
)

from DeepVisionClassifier import DeepVisionClassifier


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


class CallbackBackend(object):
    def run(self, fn, args=(), kwargs={}, env={}):
        return [fn(*args, **kwargs)] * self.num_processes()

    def num_processes(self):
        return 1


def test_mobilenet_v2(
    transform_row_func,
    get_data_path,
    read_image_and_transform_udf,
):
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    backend = SparkBackend(
        stdout=sys.stdout,
        stderr=sys.stderr,
        prefix_output_with_timestamp=True,
    )
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
            num_layers_to_fine_tune=0,
            store=store,
            backend=ctx,
            callbacks=callbacks,
            input_shapes=[[-1, 3, 224, 224]],
            num_classes=17,
            feature_cols=["image"],
            label_cols=["label"],
            optimizer_name="adam",
            loss_name="cross_entropy",
            batch_size=16,
            epochs=epochs,
            validation=0.1,
            verbose=1,
            profiler=None,
            partitions_per_process=1,
            transformation_fn=transform_row_func,
        )

        train_df, test_df = generate_data(spark, train_folder, test_folder)

        deep_vision_model = deep_vision_classifier.fit(train_df).setOutputCols(
            ["label_prob"]
        )

        argmax = udf(lambda v: float(np.argmax(v)), returnType=DoubleType())

        test_df_trans = test_df.withColumn(
            "features", read_image_and_transform_udf("image")
        )
        pred_df = deep_vision_model.setFeatureColumns(["features"]).transform(
            test_df_trans
        )
        pred_df = pred_df.withColumn("label_pred", argmax(pred_df.label_prob))
        evaluator = MulticlassClassificationEvaluator(
            predictionCol="label_pred", labelCol="label", metricName="accuracy"
        )
        accuracy = evaluator.evaluate(pred_df)
        assert accuracy > 0.5

    spark.stop()
