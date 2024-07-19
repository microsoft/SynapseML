# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest
from typing import Type
from pyspark.sql import DataFrame, types as t, functions as f
from synapse.ml.cyber.anomaly.complement_access import ComplementAccessTransformer
from synapsemltest.cyber.explain_tester import ExplainTester
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class TestComplementAccessTransformer(unittest.TestCase):
    def create_dataframe(self) -> DataFrame:
        schema = t.StructType(
            [
                t.StructField("tenant", t.StringType(), nullable=True),
                t.StructField("user", t.IntegerType(), nullable=True),
                t.StructField("res", t.IntegerType(), nullable=True),
            ],
        )

        return sc.createDataFrame(
            [
                ("t1", 0, 0),
                ("t1", 1, 1),
                ("t1", 2, 2),
                ("t1", 3, 3),
                ("t1", 4, 4),
                ("t1", 5, 5),
                ("t2", 0, 0),
                ("t2", 1, 1),
                ("t2", 2, 2),
                ("t2", 3, 3),
            ],
            schema,
        )

    def test_partitioned_complement_access_transformer(self):
        df = self.create_dataframe().cache()
        assert df is not None and df.count() > 0

        transformer = ComplementAccessTransformer("tenant", ["user", "res"], 3)
        complement_df = transformer.transform(df).cache()
        assert complement_df is not None and complement_df.count() > 0
        assert complement_df.schema == df.schema

        assert (
            complement_df.select("tenant", "user", "res").distinct().count()
            == complement_df.count()
        )
        assert complement_df.join(df, ["tenant", "user", "res"]).count() == 0

        assert (
            complement_df.filter(f.col("tenant") == "t1")
            .agg(f.max("user").alias("max_user"))
            .first()["max_user"]
            <= 5
        )

        assert (
            complement_df.filter(f.col("tenant") == "t1")
            .agg(f.max("res").alias("max_res"))
            .first()["max_res"]
            <= 5
        )

        assert (
            complement_df.filter(f.col("tenant") == "t2")
            .agg(f.max("user").alias("max_user"))
            .first()["max_user"]
            <= 3
        )

        assert (
            complement_df.filter(f.col("tenant") == "t2")
            .agg(f.max("res").alias("max_res"))
            .first()["max_res"]
            <= 3
        )

    def test_unpartitioned_complement_access_transformer(self):
        df = (
            self.create_dataframe()
            .filter(f.col("tenant") == "t1")
            .select("user", "res")
            .cache()
        )
        assert df is not None and df.count() > 0

        transformer = ComplementAccessTransformer(None, ["user", "res"], 3)
        complement_df = transformer.transform(df).cache()
        assert complement_df is not None and complement_df.count() > 0
        assert complement_df.schema == df.schema

        assert (
            complement_df.select("user", "res").distinct().count()
            == complement_df.count()
        )
        assert complement_df.join(df, ["user", "res"]).count() == 0

        assert (
            complement_df.agg(f.max("user").alias("max_user")).first()["max_user"] <= 5
        )
        assert complement_df.agg(f.max("res").alias("max_res")).first()["max_res"] <= 5


class TestComplementAccessTransformerExplain(ExplainTester):
    def test_explain(self):
        types = [str, list, int]

        def counts(c: int, tt: Type):
            return tt not in types or c > 0

        params = ["partitionKey", "indexedColNamesArr", "complementsetFactor"]

        self.check_explain(
            ComplementAccessTransformer("partition_key", ["indexed_col_names_arr"], 2),
            params,
            counts,
        )


if __name__ == "__main__":
    result = unittest.main()
