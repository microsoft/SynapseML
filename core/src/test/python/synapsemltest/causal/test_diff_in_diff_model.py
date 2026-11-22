# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import tempfile
import unittest

from synapse.ml.causal.DiffInDiffEstimator import DiffInDiffEstimator
from synapse.ml.causal.DiffInDiffModel import DiffInDiffModel
from synapse.ml.core.init_spark import init_spark

spark = init_spark()


class DiffInDiffModelSpec(unittest.TestCase):
    def test_summary_is_available_after_save_and_load(self):
        rows = []
        for treatment in range(2):
            for post_treatment in range(2):
                for index in range(10):
                    outcome = (
                        2.0
                        + 5.0 * treatment
                        + 3.0 * post_treatment
                        + 2.0 * treatment * post_treatment
                        + 0.1 * (index % 3)
                    )
                    rows.append((treatment, post_treatment, outcome))
        data = spark.createDataFrame(
            rows,
            ["treatment", "postTreatment", "outcome"],
        )
        model = DiffInDiffEstimator(
            treatmentCol="treatment",
            postTreatmentCol="postTreatment",
            outcomeCol="outcome",
        ).fit(data)

        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/diff-in-diff-model"
            model.write().save(path)
            loaded = DiffInDiffModel.load(path)

            self.assertAlmostEqual(loaded.treatmentEffect, model.treatmentEffect)
            self.assertAlmostEqual(loaded.standardError, model.standardError)


if __name__ == "__main__":
    unittest.main()
