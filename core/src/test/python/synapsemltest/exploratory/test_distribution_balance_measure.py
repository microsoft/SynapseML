# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

from synapse.ml.core.init_spark import init_spark
from synapse.ml.exploratory.DistributionBalanceMeasure import (
    DistributionBalanceMeasure,
)

spark = init_spark()


class DistributionBalanceMeasureSpec(unittest.TestCase):
    def test_reference_only_category_is_included_through_generated_wrapper(self):
        source = spark.createDataFrame(
            [("red",), ("red",), ("red",), ("green",), ("blue",)],
            ["color"],
        )
        result = (
            DistributionBalanceMeasure(
                sensitiveCols=["color"],
                referenceDistribution=[
                    {"red": 0.4, "green": 0.2, "blue": 0.2, "yellow": 0.2}
                ],
            )
            .transform(source)
            .collect()
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].FeatureName, "color")
        self.assertAlmostEqual(
            result[0].DistributionBalanceMeasure.js_dist,
            0.33841498603440373,
        )

    def test_integral_reference_probabilities_use_the_public_wrapper(self):
        source = spark.createDataFrame([("red",), ("red",)], ["color"])
        result = (
            DistributionBalanceMeasure(
                sensitiveCols=["color"],
                referenceDistribution=[{"red": 1, "unused": 0}],
            )
            .transform(source)
            .collect()
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].DistributionBalanceMeasure.js_dist, 0.0)
        self.assertEqual(result[0].DistributionBalanceMeasure.chi_sq_p_value, 1.0)


if __name__ == "__main__":
    unittest.main()
