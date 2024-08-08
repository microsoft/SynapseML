# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

import logging
from synapse.ml.core.logging.SynapseMLLogger import SynapseMLLogger
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class SampleTransformer(SynapseMLLogger):
    def __init__(self):
        super().__init__(log_level=logging.DEBUG)
        self.log_class("SampleTransformer")

    @SynapseMLLogger.log_transform()
    def transform(self, df):
        return True

    @SynapseMLLogger.log_fit()
    def fit(self, df):
        return True

    @SynapseMLLogger.log_verb()
    def test_throw(self):
        raise Exception("test exception")

    @SynapseMLLogger.log_verb(feature_name="test_logging")
    def test_feature_name(self):
        return 0


class NoInheritTransformer:
    def __init__(self):
        self._logger = SynapseMLLogger(log_level=logging.DEBUG)

    @SynapseMLLogger.log_verb_static(method_name="transform")
    def transform(self, df):
        return True

    @SynapseMLLogger.log_verb_static(method_name="fit")
    def fit(self, df):
        return True

    @SynapseMLLogger.log_verb_static()
    def test_throw(self):
        raise Exception("test exception")

    @SynapseMLLogger.log_verb_static(feature_name="test_logging")
    def test_feature_name(self):
        return 0

    def custom_logging_function(self, results, *args, **kwargs):
        return {"args": f"Arguments: {args}", "result": str(results)}

    @SynapseMLLogger.log_verb_static(custom_log_function=custom_logging_function)
    def test_custom_function(self, df):
        return 0

    def custom_logging_function_w_collision(self, results, *args, **kwargs):
        return {
            "args": f"Arguments: {args}",
            "result": str(results),
            "className": "this is the collision key",
        }

    @SynapseMLLogger.log_verb_static(
        custom_log_function=custom_logging_function_w_collision
    )
    def test_custom_function_w_collision(self, df):
        return 0


class LoggingTest(unittest.TestCase):
    def test_logging_smoke(self):
        t = SampleTransformer()
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        df = sc.createDataFrame(data, columns)
        t.transform(df)
        t.fit(df)
        try:
            t.test_throw()
        except Exception as e:
            assert f"{e}" == "test exception"
        t.test_feature_name()

    def test_log_verb_static(self):
        t = NoInheritTransformer()
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        df = sc.createDataFrame(data, columns)
        t.transform(df)
        t.fit(df)
        t.test_feature_name()
        t.test_custom_function(df)
        try:
            t.test_throw()
        except Exception as e:
            assert f"{e}" == "test exception"
        try:
            t.test_custom_function_w_collision(df)
        except Exception as e:
            assert (
                f"{e}" == "Shared keys found in custom logger dictionary: {'className'}"
            )


if __name__ == "__main__":
    result = unittest.main()
