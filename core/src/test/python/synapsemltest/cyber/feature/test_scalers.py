# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest
from typing import Type
from pyspark.sql import functions as f, types as t
from synapse.ml.cyber.feature import LinearScalarScaler, StandardScalarScaler
from synapsemltest.cyber.explain_tester import ExplainTester
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class TestScalers(unittest.TestCase):
    def create_sample_dataframe(self):
        schema = t.StructType(
            [
                t.StructField("tenant", t.StringType(), nullable=True),
                t.StructField("name", t.StringType(), nullable=True),
                t.StructField("score", t.FloatType(), nullable=True),
            ],
        )

        return sc.createDataFrame(
            [
                ("t1", "5", 500.0),
                ("t1", "6", 600.0),
                ("t2", "7", 700.0),
                ("t2", "8", 800.0),
                ("t3", "9", 900.0),
            ],
            schema,
        )

    def test_unpartitioned_min_max_scaler(self):
        ls = LinearScalarScaler("score", None, "new_score", 5, 9, use_pandas=False)

        df = self.create_sample_dataframe().cache()
        model = ls.fit(df)
        new_df = model.transform(df).cache()

        assert new_df.count() == df.count()

        assert (
            0
            == new_df.filter(
                f.col("name").cast(t.IntegerType())
                != f.col("new_score").cast(t.IntegerType()),
            ).count()
        )

    def test_partitioned_min_max_scaler(self):
        ls = LinearScalarScaler("score", "tenant", "new_score", 1, 2, use_pandas=False)

        df = self.create_sample_dataframe()
        model = ls.fit(df)
        new_df = model.transform(df)

        assert new_df.count() == df.count()

        t1_arr = new_df.filter(f.col("tenant") == "t1").orderBy("new_score").collect()
        assert len(t1_arr) == 2
        assert t1_arr[0]["new_score"] == 1.0
        assert t1_arr[1]["new_score"] == 2.0

        t2_arr = new_df.filter(f.col("tenant") == "t2").orderBy("new_score").collect()
        assert len(t2_arr) == 2
        assert t2_arr[0]["new_score"] == 1.0
        assert t2_arr[1]["new_score"] == 2.0

        t3_arr = new_df.filter(f.col("tenant") == "t3").orderBy("new_score").collect()
        assert len(t3_arr) == 1
        # this is the average between min and max
        assert t3_arr[0]["new_score"] == 1.5

    def test_unpartitioned_standard_scaler(self):
        ls = StandardScalarScaler("score", None, "new_score", 1.0, use_pandas=False)

        df = self.create_sample_dataframe()
        model = ls.fit(df)
        new_df = model.transform(df)

        assert new_df.count() == df.count()

        new_scores = new_df.toPandas()["new_score"]

        assert new_scores.to_numpy().mean() == 0.0
        assert abs(new_scores.to_numpy().std() - 1.0) < 0.0001

    def test_partitioned_standard_scaler(self):
        ls = StandardScalarScaler("score", "tenant", "new_score", 1.0, use_pandas=False)

        df = self.create_sample_dataframe()
        model = ls.fit(df)
        new_df = model.transform(df)

        assert new_df.count() == df.count()

        for tenant in ["t1", "t2", "t3"]:
            new_scores = new_df.filter(f.col("tenant") == tenant).toPandas()[
                "new_score"
            ]

            assert new_scores is not None

            the_mean = new_scores.to_numpy().mean()
            the_std = new_scores.to_numpy().std()
            tenant_scores = [s for _, s in new_scores.items()]

            assert the_mean == 0.0

            if tenant != "t3":
                assert abs(the_std - 1.0) < 0.0001, str(the_std)
                assert len(tenant_scores) == 2
                assert tenant_scores[0] == -1.0
                assert tenant_scores[1] == 1.0
            else:
                assert the_std == 0.0
                assert len(tenant_scores) == 1
                assert tenant_scores[0] == 0.0


class TestStandardScalarScalerExplain(ExplainTester):
    def test_explain(self):
        types = [str, float]

        def counts(c: int, tt: Type):
            return tt not in types or c > 0

        params = ["inputCol", "partitionKey", "outputCol", "coefficientFactor"]
        self.check_explain(
            StandardScalarScaler("input", "tenant", "output"),
            params,
            counts,
        )


class TestLinearScalarScalerExplain(ExplainTester):
    def test_explain(self):
        types = [str, float]

        def counts(c: int, tt: Type):
            return tt not in types or c > 0

        params = [
            "inputCol",
            "partitionKey",
            "outputCol",
            "minRequiredValue",
            "maxRequiredValue",
        ]
        self.check_explain(
            LinearScalarScaler("input", "tenant", "output"),
            params,
            counts,
        )


if __name__ == "__main__":
    result = unittest.main()
