# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import tempfile
import unittest
from pathlib import Path

from synapse.ml.core.init_spark import init_spark
from synapse.ml.stages import EnsembleByKey

spark = init_spark()


class EnsembleByKeySpec(unittest.TestCase):
    def test_col_names_follow_params_after_transform_and_load(self):
        frame = spark.createDataFrame(
            [("group", 1.0, 2.0), ("group", 3.0, 4.0)],
            ["key", "score", "other"],
        )
        with self.assertRaisesRegex(Exception, "keys must be set and non-empty"):
            EnsembleByKey(keys=[], cols=["score"]).transform(frame)
        with self.assertRaisesRegex(Exception, "cols must be set and non-empty"):
            EnsembleByKey(keys=["key"], cols=[]).transform(frame)

        transformer = EnsembleByKey(keys=["key"], cols=["score"])

        self.assertEqual(transformer.getColNames(), ["mean(score)"])
        self.assertFalse(transformer.isSet(transformer.colNames))
        self.assertFalse(transformer.hasDefault(transformer.colNames))
        transformer.transform(frame).collect()
        self.assertFalse(transformer.isSet(transformer.colNames))
        self.assertFalse(transformer.hasDefault(transformer.colNames))

        transformer.setCols(["score", "other"])
        self.assertEqual(transformer.getColNames(), ["mean(score)", "mean(other)"])

        with tempfile.TemporaryDirectory() as directory:
            model_path = str(Path(directory) / "ensemble-by-key")
            transformer.write().save(model_path)
            loaded = EnsembleByKey.load(model_path)

        self.assertEqual(loaded.getColNames(), ["mean(score)", "mean(other)"])
        self.assertFalse(loaded.isSet(loaded.colNames))
        self.assertFalse(loaded.hasDefault(loaded.colNames))

        transformer.setColNames(["average-score", "average-other"])
        with tempfile.TemporaryDirectory() as directory:
            model_path = str(Path(directory) / "ensemble-by-key-explicit")
            transformer.write().save(model_path)
            loaded = EnsembleByKey.load(model_path)

        self.assertEqual(loaded.getColNames(), ["average-score", "average-other"])
        self.assertTrue(loaded.isSet(loaded.colNames))


if __name__ == "__main__":
    unittest.main()
