# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pytorch_lightning.callbacks import ModelCheckpoint
from synapse.ml.dl import *

import pytest
from .conftest import CallbackBackend, _prepare_text_data
from .test_deep_vision_classifier import local_store
from .test_deep_vision_model import MyDummyCallback


@pytest.mark.skip(reason="not testing this for now")
def test_bert_base_cased():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    train_df, test_df = _prepare_text_data(spark)

    ctx = CallbackBackend()

    epochs = 2
    callbacks = [
        MyDummyCallback(epochs),
        ModelCheckpoint(dirpath="target/bert_base_uncased/"),
    ]

    with local_store() as store:
        checkpoint = "bert-base-uncased"

        deep_text_classifier = DeepTextClassifier(
            checkpoint=checkpoint,
            store=store,
            backend=ctx,
            callbacks=callbacks,
            num_classes=6,
            batch_size=16,
            epochs=epochs,
            validation=0.1,
            text_col="Text",
            transformation_removed_fields=["Text", "Emotion", "label"],
        )

        deep_text_model = deep_text_classifier.fit(train_df)

        pred_df = deep_text_model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            predictionCol="prediction", labelCol="label", metricName="accuracy"
        )
        accuracy = evaluator.evaluate(pred_df)
        assert accuracy > 0.5

    spark.stop()
