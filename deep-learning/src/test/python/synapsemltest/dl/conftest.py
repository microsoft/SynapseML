# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import shutil
import subprocess
import urllib
from os.path import join

import numpy as np
import pandas as pd
import pytest
import torchvision.transforms as transforms
from pyspark.ml.feature import StringIndexer

IS_WINDOWS = os.name == "nt"
delimiter = "\\" if IS_WINDOWS else "/"
dataset_dir = (
    delimiter.join([os.getcwd(), os.pardir, os.pardir, os.pardir, os.pardir])
    + delimiter
)


class CallbackBackend(object):
    def run(self, fn, args=(), kwargs={}, env={}):
        return [fn(*args, **kwargs)] * self.num_processes()

    def num_processes(self):
        return 1


def _download_dataset():
    urllib.request.urlretrieve(
        "https://mmlspark.blob.core.windows.net/publicwasb/17flowers.tgz",
        dataset_dir + "17flowers.tgz",
    )
    if os.path.exists(dataset_dir + "jpg"):
        shutil.rmtree(dataset_dir + "jpg")

    command = "tar -xzf {}17flowers.tgz -C {} \n".format(dataset_dir, dataset_dir)
    subprocess.run(command.split(), stdout=subprocess.PIPE)
    os.remove(dataset_dir + "17flowers.tgz")
    files = [
        join(dp, f)
        for dp, dn, filenames in os.walk(dataset_dir + "jpg")
        for f in filenames
        if os.path.splitext(f)[1] == ".jpg"
    ]
    assert len(files) == 1360
    np.random.shuffle(files)
    train_files, test_files = np.split(np.array(files), [int(len(files) * 0.75)])
    train_dir = dataset_dir + "jpg{}train".format(delimiter)
    test_dir = dataset_dir + "jpg{}test".format(delimiter)
    if not os.path.exists(train_dir):
        os.makedirs(train_dir)
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    for name in train_files:
        path, image = (
            delimiter.join(name.split(delimiter)[:-1]),
            name.split(delimiter)[-1],
        )
        shutil.move(name, delimiter.join([path, "train", image]))

    for name in test_files:
        path, image = (
            delimiter.join(name.split(delimiter)[:-1]),
            name.split(delimiter)[-1],
        )
        shutil.move(name, delimiter.join([path, "test", image]))


@pytest.fixture(scope="module")
def get_data_path():
    if not os.path.exists(join(dataset_dir, "jpg", delimiter, "train")):
        _download_dataset()
    train_folder = dataset_dir + "jpg" + delimiter + "train"
    test_folder = dataset_dir + "jpg" + delimiter + "test"
    return train_folder, test_folder


@pytest.fixture(scope="module")
def transform():
    transform = transforms.Compose(
        [
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    return transform


def _prepare_text_data(spark):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .load(
            "wasbs://publicwasb@mmlspark.blob.core.windows.net/text_classification/Emotion_classification.csv"
        )
    )
    indexer = StringIndexer(inputCol="Emotion", outputCol="label")
    indexer_model = indexer.fit(df)
    df = indexer_model.transform(df).drop("Emotion")

    train_df, test_df = df.randomSplit([0.85, 0.15], seed=1)

    return train_df, test_df
