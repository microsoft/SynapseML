# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

import logging
from synapse.ml.core.logging.SynapseMLLogger import SynapseMLLogger
from synapsemltest.spark import *


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


if __name__ == "__main__":
    result = unittest.main()
