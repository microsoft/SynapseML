# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import importlib
import os
import zipfile
from pathlib import Path

import numpy as np
import pytest
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from synapse.ml.opencv.ImageTransformer import (
    ImageSchema,
    ImageTransformer,
    toImage,
    toNDArray,
)


@pytest.fixture(scope="module", autouse=True)
def check_candidate_wheel():
    wheel = os.environ.get("SYNAPSEML_CONSUMER_WHEEL")
    if wheel:
        with zipfile.ZipFile(wheel) as archive:
            for name in (
                "synapse.ml.opencv.ImageTransformer",
                "synapse.ml.opencv._ImageTransformer",
                "synapse.ml.core",
            ):
                module = importlib.import_module(name)
                member = name.replace(".", "/")
                member += "/__init__.py" if hasattr(module, "__path__") else ".py"
                assert Path(module.__file__).read_bytes() == archive.read(member)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    owns_context = SparkContext._active_spark_context is None
    session = (
        SparkSession.builder.master("local[2]").appName("ImageConversion").getOrCreate()
    )
    try:
        yield session
    finally:
        if owns_context:
            session.stop()


@pytest.mark.parametrize("container", [bytes, bytearray, list, np.array, memoryview])
@pytest.mark.parametrize("channels", [1, 3])
def test_image_binary_and_array_inputs_preserve_shape_and_color(container, channels):
    raw = bytes(range(1, 2 * channels + 1))
    data = container(raw if container is memoryview else list(raw))
    image = Row(height=1, width=2, nChannels=channels, data=data)
    expected = np.arange(1, 2 * channels + 1, dtype=np.uint8).reshape(1, 2, channels)
    if channels == 3:
        expected = expected[:, :, ::-1]
    actual = toNDArray(image)
    assert actual.dtype == np.uint8
    np.testing.assert_array_equal(actual, expected)
    if container is np.array:
        assert list(data) == list(raw)
    else:
        assert bytes(data) == raw


def test_rgb_image_round_trip():
    pixels = np.array([[[0, 128, 255], [32, 64, 96]]], dtype=np.uint8)
    image = toImage(pixels)
    np.testing.assert_array_equal(toNDArray(image), pixels)
    binary_image = Row(**{**image.asDict(), "data": bytes(image.data)})
    np.testing.assert_array_equal(toNDArray(binary_image), pixels)


@pytest.mark.parametrize("data", [b"", b"\x01\x02", bytearray(b"\x01\x02")])
def test_invalid_image_size_is_rejected(data):
    with pytest.raises(ValueError):
        toNDArray(Row(height=1, width=2, nChannels=3, data=data))


def image_frame(spark):
    pixels = np.array([[[0, 128, 255], [32, 64, 96]]], dtype=np.uint8)
    frame = spark.createDataFrame(
        [(toImage(pixels),)], StructType([StructField("image", ImageSchema)])
    )
    return frame, pixels


def test_spark_binary_image_round_trip(spark):
    frame, pixels = image_frame(spark)
    image = frame.first().image
    assert isinstance(image.data, (bytes, bytearray))
    np.testing.assert_array_equal(toNDArray(image), pixels)


def test_packaged_transformer_and_persistence(spark, tmp_path):
    jar = os.environ.get("SYNAPSEML_CONSUMER_JAR")
    if jar:
        loader = (
            spark.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader()
        )
        clazz = loader.loadClass(
            "com.microsoft.azure.synapse.ml.opencv.ImageTransformer"
        )
        location = clazz.getProtectionDomain().getCodeSource().getLocation().toURI()
        assert Path(location.getPath()).resolve() == Path(jar).resolve()
    frame, _ = image_frame(spark)
    transformer = ImageTransformer().setInputCol("image").setOutputCol("resized")
    transformer.resize((1, 1))
    result = toNDArray(transformer.transform(frame).first().resized)
    assert result.shape == (1, 1, 3)
    saved = str(tmp_path / "image-transformer")
    transformer.write().save(saved)
    loaded = ImageTransformer.load(saved)
    np.testing.assert_array_equal(
        toNDArray(loaded.transform(frame).first().resized), result
    )
