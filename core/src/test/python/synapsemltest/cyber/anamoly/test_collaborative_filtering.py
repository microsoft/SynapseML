# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import tempfile
import unittest
from typing import Dict, Optional, Set, Type, Union
from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame, types as t, functions as f
from synapse.ml.cyber.feature import indexers
from synapse.ml.cyber import DataFactory
from synapse.ml.cyber.anomaly.collaborative_filtering import (
    AccessAnomaly,
    AccessAnomalyModel,
    AccessAnomalyConfig,
    ConnectedComponents,
    ModelNormalizeTransformer,
    _UserResourceFeatureVectorMapping as UserResourceFeatureVectorMapping,
)

from synapsemltest.cyber.explain_tester import ExplainTester
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)
epsilon = 10**-3


def materialized_cache(df: DataFrame) -> DataFrame:
    return df.sql_ctx.createDataFrame(df.toPandas(), df.schema).coalesce(1).cache()


class BasicStats:
    def __init__(
        self,
        count: int,
        min_value: float,
        max_value: float,
        mean: float,
        std: float,
    ):
        self.count = count
        self.min = min_value
        self.max = max_value
        self.mean = mean
        self.std = std

    def as_dict(self):
        return {
            "count": self.count,
            "min": self.min,
            "max": self.max,
            "mean": self.mean,
            "std": self.std,
        }

    def __repr__(self):
        return str(self.as_dict())


class StatsMap:
    def __init__(self, stats_map: Dict[str, BasicStats]):
        self.stats_map = stats_map

    def get_stat(self, tenant):
        return self.stats_map.get(tenant)

    def get_tenants(self) -> Set[str]:
        return set(self.stats_map.keys())

    def get_stats(self) -> Set[BasicStats]:
        return set(self.stats_map.values())

    def __repr__(self):
        return self.stats_map.__repr__()


def create_stats(
    df,
    tenant_col: str,
    value_col: str = AccessAnomalyConfig.default_output_col,
) -> StatsMap:
    stat_rows = (
        df.groupBy(tenant_col)
        .agg(
            f.count("*").alias("__count__"),
            f.min(f.col(value_col)).alias("__min__"),
            f.max(f.col(value_col)).alias("__max__"),
            f.mean(f.col(value_col)).alias("__mean__"),
            f.stddev_pop(f.col(value_col)).alias("__std__"),
        )
        .collect()
    )

    stats_map = {
        row[tenant_col]: BasicStats(
            row["__count__"],
            row["__min__"],
            row["__max__"],
            row["__mean__"],
            row["__std__"],
        )
        for row in stat_rows
    }

    return StatsMap(stats_map)


def get_department(the_col: Union[str, f.Column]) -> f.Column:
    _the_col = the_col if isinstance(the_col, f.Column) else f.col(the_col)
    return f.element_at(f.split(_the_col, "_"), 1)


def without_ffa(df: DataFrame, res_col_p: Optional[str] = None) -> DataFrame:
    res_col = AccessAnomalyConfig.default_res_col if res_col_p is None else res_col_p
    return df.filter(get_department(res_col) != "ffa")


single_component_data = True


def create_data_factory():
    return DataFactory(single_component=single_component_data)


class Dataset:
    def __init__(self):
        num_tenants = 2

        self.training = None
        self.intra_test = None
        self.inter_test = None

        for tid in range(num_tenants):
            factory = create_data_factory()

            training_pdf = factory.create_clustered_training_data(0.25)
            intra_test_pdf = factory.create_clustered_intra_test_data(training_pdf)
            inter_test_pdf = factory.create_clustered_inter_test_data()

            print(type(training_pdf))
            curr_training = spark.createDataFrame(training_pdf).withColumn(
                AccessAnomalyConfig.default_tenant_col,
                f.lit(tid),
            )

            curr_intra_test = spark.createDataFrame(intra_test_pdf).withColumn(
                AccessAnomalyConfig.default_tenant_col,
                f.lit(tid),
            )

            curr_inter_test = spark.createDataFrame(inter_test_pdf).withColumn(
                AccessAnomalyConfig.default_tenant_col,
                f.lit(tid),
            )

            self.training = (
                self.training.union(curr_training)
                if self.training is not None
                else curr_training
            )
            self.intra_test = (
                self.intra_test.union(curr_intra_test)
                if self.intra_test is not None
                else curr_intra_test
            )
            self.inter_test = (
                self.inter_test.union(curr_inter_test)
                if self.inter_test is not None
                else curr_inter_test
            )

        self.training = materialized_cache(self.training)
        self.intra_test = materialized_cache(self.intra_test)
        self.inter_test = materialized_cache(self.inter_test)

        assert (
            without_ffa(
                self.training.join(
                    self.intra_test,
                    [
                        AccessAnomalyConfig.default_tenant_col,
                        AccessAnomalyConfig.default_user_col,
                        AccessAnomalyConfig.default_res_col,
                    ],
                ),
            ).count()
            == 0
        ), f"self.training.join is not 0"

        self.num_users = (
            self.training.select(
                AccessAnomalyConfig.default_tenant_col,
                AccessAnomalyConfig.default_user_col,
            )
            .distinct()
            .count()
        )

        self.num_resources = (
            self.training.select(
                AccessAnomalyConfig.default_tenant_col,
                AccessAnomalyConfig.default_res_col,
            )
            .distinct()
            .count()
        )

        self.default_access_anomaly_model = None
        self.per_res_adjust_access_anomaly_model = None
        self.implicit_access_anomaly_model = None

    @staticmethod
    def create_new_training(ratio: float) -> DataFrame:
        training_pdf = create_data_factory().create_clustered_training_data(ratio)

        return materialized_cache(
            spark.createDataFrame(training_pdf).withColumn(
                AccessAnomalyConfig.default_tenant_col,
                f.lit(0),
            ),
        )

    def get_default_access_anomaly_model(self):
        if self.default_access_anomaly_model is not None:
            return self.default_access_anomaly_model

        access_anomaly = AccessAnomaly(
            tenantCol=AccessAnomalyConfig.default_tenant_col,
            maxIter=10,
        )
        self.default_access_anomaly_model = access_anomaly.fit(self.training)
        self.default_access_anomaly_model.preserve_history = False
        return self.default_access_anomaly_model


data_set = Dataset()


def get_department(the_col: Union[str, f.Column]) -> f.Column:
    _the_col = the_col if isinstance(the_col, f.Column) else f.col(the_col)
    return f.element_at(f.split(_the_col, "_"), 1)


class TestModelNormalizeTransformer(unittest.TestCase):
    def test_model_standard_scaling(self):
        tenant_col = AccessAnomalyConfig.default_tenant_col
        user_col = AccessAnomalyConfig.default_user_col
        user_vec_col = AccessAnomalyConfig.default_user_col + "_vec"
        res_col = AccessAnomalyConfig.default_res_col
        res_vec_col = AccessAnomalyConfig.default_res_col + "_vec"
        likelihood_col = AccessAnomalyConfig.default_likelihood_col

        df_schema = t.StructType(
            [
                t.StructField(tenant_col, t.StringType(), False),
                t.StructField(user_col, t.StringType(), False),
                t.StructField(res_col, t.StringType(), False),
                t.StructField(likelihood_col, t.DoubleType(), False),
            ],
        )

        user_model_schema = t.StructType(
            [
                t.StructField(tenant_col, t.StringType(), False),
                t.StructField(user_col, t.StringType(), False),
                t.StructField(user_vec_col, t.ArrayType(t.DoubleType()), False),
            ],
        )

        res_model_schema = t.StructType(
            [
                t.StructField(tenant_col, t.StringType(), False),
                t.StructField(res_col, t.StringType(), False),
                t.StructField(res_vec_col, t.ArrayType(t.DoubleType()), False),
            ],
        )

        df = materialized_cache(
            spark.createDataFrame(
                [["0", "roy", "res1", 4.0], ["0", "roy", "res2", 8.0]],
                df_schema,
            ),
        )

        user_mapping_df = materialized_cache(
            spark.createDataFrame(
                [["0", "roy", [1.0, 1.0, 0.0, 1.0]]], user_model_schema
            ),
        )

        res_mapping_df = materialized_cache(
            spark.createDataFrame(
                [
                    ["0", "res1", [2.0, 2.0, 1.0, 0.0]],
                    ["0", "res2", [4.0, 4.0, 1.0, 0.0]],
                ],
                res_model_schema,
            ),
        )

        user_res_feature_vector_mapping_df = UserResourceFeatureVectorMapping(
            tenant_col,
            user_col,
            user_vec_col,
            res_col,
            res_vec_col,
            None,
            None,
            None,
            user_mapping_df,
            res_mapping_df,
        )

        assert user_res_feature_vector_mapping_df.check()

        model_normalizer = ModelNormalizeTransformer(df, 2)
        fixed_user_res_mapping_df = model_normalizer.transform(
            user_res_feature_vector_mapping_df,
        )

        assert fixed_user_res_mapping_df.check()
        assert (
            fixed_user_res_mapping_df.user_feature_vector_mapping_df.count()
            == user_mapping_df.count()
        )
        assert (
            fixed_user_res_mapping_df.res_feature_vector_mapping_df.count()
            == res_mapping_df.count()
        )

        fixed_user_mapping_df = fixed_user_res_mapping_df.user_feature_vector_mapping_df
        fixed_res_mapping_df = fixed_user_res_mapping_df.res_feature_vector_mapping_df

        assert (
            fixed_user_res_mapping_df.user_feature_vector_mapping_df.filter(
                f.size(f.col(user_vec_col)) == 4,
            ).count()
            == user_mapping_df.count()
        ), f"{fixed_user_mapping_df.filter(f.size(f.col(user_vec_col)) == 4).count()} != {user_mapping_df.count()}"

        assert (
            fixed_user_res_mapping_df.res_feature_vector_mapping_df.filter(
                f.size(f.col(res_vec_col)) == 4,
            ).count()
            == res_mapping_df.count()
        ), f"{fixed_res_mapping_df.filter(f.size(f.col(res_vec_col)) == 4).count()} != {res_mapping_df.count()}"

        user_vectors = [
            row[user_vec_col]
            for row in fixed_user_res_mapping_df.user_feature_vector_mapping_df.collect()
        ]
        assert len(user_vectors) == 1
        assert user_vectors[0] == [-0.5, -0.5, 3.0, -0.5]

    def test_model_end2end(self):
        num_users = 10
        num_resources = 25

        tenant_col = AccessAnomalyConfig.default_tenant_col
        user_col = AccessAnomalyConfig.default_user_col
        user_vec_col = AccessAnomalyConfig.default_user_col + "_vec"
        res_col = AccessAnomalyConfig.default_res_col
        res_vec_col = AccessAnomalyConfig.default_res_col + "_vec"
        likelihood_col = AccessAnomalyConfig.default_likelihood_col
        output_col = AccessAnomalyConfig.default_output_col

        user_model_schema = t.StructType(
            [
                t.StructField(tenant_col, t.StringType(), False),
                t.StructField(user_col, t.StringType(), False),
                t.StructField(user_vec_col, t.ArrayType(t.DoubleType()), False),
            ],
        )

        res_model_schema = t.StructType(
            [
                t.StructField(tenant_col, t.StringType(), False),
                t.StructField(res_col, t.StringType(), False),
                t.StructField(res_vec_col, t.ArrayType(t.DoubleType()), False),
            ],
        )

        user_mapping_df = materialized_cache(
            spark.createDataFrame(
                [
                    [
                        0,
                        "roy_{0}".format(i),
                        [float(i % 10), float((i + 1) % (num_users / 2)), 0.0, 1.0],
                    ]
                    for i in range(num_users)
                ],
                user_model_schema,
            ),
        )

        res_mapping_df = materialized_cache(
            spark.createDataFrame(
                [
                    [
                        0,
                        "res_{0}".format(i),
                        [float(i % 10), float((i + 1) % num_resources / 2), 1.0, 0.0],
                    ]
                    for i in range(num_resources)
                ],
                res_model_schema,
            ),
        )

        df = (
            user_mapping_df.select(tenant_col, user_col)
            .distinct()
            .join(res_mapping_df.select(tenant_col, res_col).distinct(), tenant_col)
            .withColumn(likelihood_col, f.lit(0.0))
        )

        assert df.count() == num_users * num_resources

        user_mapping_df = materialized_cache(
            spark.createDataFrame(
                [
                    [
                        0,
                        "roy_{0}".format(i),
                        [float(i % 10), float((i + 1) % (num_users / 2)), 0.0, 1.0],
                    ]
                    for i in range(num_users)
                ],
                user_model_schema,
            ),
        )

        res_mapping_df = materialized_cache(
            spark.createDataFrame(
                [
                    [
                        0,
                        "res_{0}".format(i),
                        [float(i % 10), float((i + 1) % num_resources / 2), 1.0, 0.0],
                    ]
                    for i in range(num_resources)
                ],
                res_model_schema,
            ),
        )

        df = (
            user_mapping_df.select(tenant_col, user_col)
            .distinct()
            .join(res_mapping_df.select(tenant_col, res_col).distinct(), tenant_col)
            .withColumn(likelihood_col, f.lit(0.0))
        )

        assert df.count() == num_users * num_resources

        user_res_cf_mapping = UserResourceFeatureVectorMapping(
            tenant_col,
            user_col,
            user_vec_col,
            res_col,
            res_vec_col,
            df.select(tenant_col, user_col, res_col),
            None,
            None,
            user_mapping_df,
            res_mapping_df,
        )

        assert user_res_cf_mapping.check()

        model_normalizer = ModelNormalizeTransformer(df, 2)

        user_res_norm_cf_mapping = model_normalizer.transform(user_res_cf_mapping)

        assert user_res_cf_mapping.check()
        assert (
            user_res_norm_cf_mapping.user_feature_vector_mapping_df.count()
            == user_mapping_df.count()
        )
        assert (
            user_res_norm_cf_mapping.res_feature_vector_mapping_df.count()
            == res_mapping_df.count()
        )

        assert (
            user_res_norm_cf_mapping.user_feature_vector_mapping_df.filter(
                f.size(f.col(user_vec_col)) == 4,
            ).count()
            == user_mapping_df.count()
        )

        assert (
            user_res_norm_cf_mapping.res_feature_vector_mapping_df.filter(
                f.size(f.col(res_vec_col)) == 4,
            ).count()
            == res_mapping_df.count()
        )

        for preserve_history in [False, True]:
            an_model = AccessAnomalyModel(user_res_norm_cf_mapping, output_col)
            an_model.preserve_history = preserve_history

            fixed_df = an_model.transform(df)
            assert fixed_df is not None
            assert fixed_df.count() == df.count()

            stats_map = create_stats(fixed_df, tenant_col, output_col)

            for stats in stats_map.get_stats():
                if not preserve_history:
                    assert stats.min < -epsilon, stats
                    assert stats.max > epsilon, stats
                    assert abs(stats.mean) < epsilon, stats
                    assert abs(stats.std - 1.0) < epsilon, stats
                else:
                    assert stats.min == 0.0 and stats.max == 0.0


class TestAccessAnomalyExplain(ExplainTester):
    def test_explain(self):
        params = [
            "tenantCol",
            "userCol",
            "resCol",
            "likelihoodCol",
            "outputCol",
            "rankParam",
            "maxIter",
            "regParam",
            "numBlocks",
            "separateTenants",
            "lowValue",
            "highValue",
            "applyImplicitCf",
            "alphaParam",
            "complementsetFactor",
            "negScore",
            "historyAccessDf",
        ]

        types = [str, int, float, None]

        def counts(c: int, tt: Type):
            return tt not in types or c > 0

        self.check_explain(
            AccessAnomaly(tenantCol=AccessAnomalyConfig.default_tenant_col),
            params,
            counts,
        )


class TestAccessAnomaly(unittest.TestCase):
    def test_save_and_load(self):
        model = data_set.get_default_access_anomaly_model()

        with tempfile.TemporaryDirectory() as tmpdirname:
            model.save(tmpdirname)
            loaded_model = AccessAnomalyModel.load(sc, tmpdirname)
            assert loaded_model is not None

            tenant_col = model.tenant_col
            user_col = model.user_col
            user_vec_col = model.user_vec_col
            res_col = model.res_col
            res_vec_col = model.res_vec_col

            assert loaded_model.tenant_col == tenant_col
            assert loaded_model.user_col == user_col
            assert loaded_model.user_vec_col == user_vec_col
            assert loaded_model.res_col == res_col
            assert loaded_model.res_vec_col == res_vec_col

            intra_test_scored = model.transform(data_set.intra_test).orderBy(
                tenant_col,
                user_col,
                res_col,
            )
            intra_test_scored_tag = loaded_model.transform(data_set.intra_test).orderBy(
                tenant_col,
                user_col,
                res_col,
            )

            assert_frame_equal(
                intra_test_scored.toPandas(),
                intra_test_scored_tag.toPandas(),
            )

            inter_test_scored = model.transform(data_set.inter_test).orderBy(
                tenant_col,
                user_col,
                res_col,
            )
            inter_test_scored_tag = loaded_model.transform(data_set.inter_test).orderBy(
                tenant_col,
                user_col,
                res_col,
            )

            assert_frame_equal(
                inter_test_scored.toPandas(),
                inter_test_scored_tag.toPandas(),
            )

    def test_enrich_and_normalize(self):
        training = Dataset.create_new_training(1.0).cache()

        access_anomaly = AccessAnomaly(
            tenantCol=AccessAnomalyConfig.default_tenant_col,
            maxIter=10,
            applyImplicitCf=False,
        )

        tenant_col = access_anomaly.tenant_col
        user_col = access_anomaly.user_col
        indexed_user_col = access_anomaly.indexed_user_col
        res_col = access_anomaly.res_col
        indexed_res_col = access_anomaly.indexed_res_col
        scaled_likelihood_col = access_anomaly.scaled_likelihood_col

        assert training.filter(f.col(user_col).isNull()).count() == 0
        assert training.filter(f.col(res_col).isNull()).count() == 0

        the_indexer = indexers.MultiIndexer(
            indexers=[
                indexers.IdIndexer(
                    input_col=user_col,
                    partition_key=tenant_col,
                    output_col=indexed_user_col,
                    reset_per_partition=False,
                ),
                indexers.IdIndexer(
                    input_col=res_col,
                    partition_key=tenant_col,
                    output_col=indexed_res_col,
                    reset_per_partition=False,
                ),
            ],
        )

        the_indexer_model = the_indexer.fit(training)
        indexed_df = materialized_cache(the_indexer_model.transform(training))

        assert indexed_df.filter(f.col(indexed_user_col).isNull()).count() == 0
        assert indexed_df.filter(f.col(indexed_res_col).isNull()).count() == 0
        assert indexed_df.filter(f.col(indexed_user_col) <= 0).count() == 0
        assert indexed_df.filter(f.col(indexed_res_col) <= 0).count() == 0

        unindexed_df = materialized_cache(the_indexer_model.undo_transform(indexed_df))
        assert unindexed_df.filter(f.col(user_col).isNull()).count() == 0
        assert unindexed_df.filter(f.col(res_col).isNull()).count() == 0

        enriched_indexed_df = materialized_cache(
            access_anomaly._enrich_and_normalize(indexed_df),
        )
        enriched_df = materialized_cache(
            without_ffa(the_indexer_model.undo_transform(enriched_indexed_df)),
        )

        assert enriched_df.filter(f.col(user_col).isNull()).count() == 0
        assert enriched_df.filter(f.col(res_col).isNull()).count() == 0

        assert (
            enriched_df.filter(
                (get_department(user_col) == get_department(res_col))
                & (f.col(scaled_likelihood_col) == 1.0),
            ).count()
            == 0
        )

        assert (
            enriched_df.filter(
                (get_department(user_col) != get_department(res_col))
                & (f.col(scaled_likelihood_col) != 1.0),
            ).count()
            == 0
        )

        assert (
            enriched_df.filter(
                (get_department(user_col) != get_department(res_col)),
            ).count()
            == enriched_df.filter(f.col(scaled_likelihood_col) == 1.0).count()
        )

        assert (
            enriched_df.filter(
                (get_department(user_col) == get_department(res_col)),
            ).count()
            == enriched_df.filter(f.col(scaled_likelihood_col) != 1.0).count()
        )

        low_value = access_anomaly.low_value
        high_value = access_anomaly.high_value

        assert enriched_df.count() > training.count()
        assert (
            enriched_df.filter(
                (
                    (f.col(scaled_likelihood_col) >= low_value)
                    & (f.col(scaled_likelihood_col) <= high_value)
                )
                | (f.col(scaled_likelihood_col) == 1.0),
            ).count()
            == enriched_df.count()
        )

    def test_mean_and_std(self):
        model = data_set.get_default_access_anomaly_model()

        assert (
            model.user_mapping_df.select(
                AccessAnomalyConfig.default_tenant_col,
                AccessAnomalyConfig.default_user_col,
            )
            .distinct()
            .count()
            == data_set.num_users
        )

        assert (
            model.res_mapping_df.select(
                AccessAnomalyConfig.default_tenant_col,
                AccessAnomalyConfig.default_res_col,
            )
            .distinct()
            .count()
            == data_set.num_resources
        )

        res_df = materialized_cache(model.transform(data_set.training))
        assert res_df is not None
        assert data_set.training.count() == res_df.count()
        assert (
            res_df.filter(
                f.col(AccessAnomalyConfig.default_output_col).isNull(),
            ).count()
            == 0
        )

        stats_map = create_stats(res_df, AccessAnomalyConfig.default_tenant_col)

        for stats in stats_map.get_stats():
            assert stats.min < -epsilon, stats
            assert stats.max > epsilon, stats
            assert abs(stats.mean) < epsilon, stats
            assert abs(stats.std - 1.0) < epsilon, stats

    def test_data_match_for_cf(self):
        tenant_col = AccessAnomalyConfig.default_tenant_col
        user_col = AccessAnomalyConfig.default_user_col
        res_col = AccessAnomalyConfig.default_res_col

        df1 = materialized_cache(
            without_ffa(data_set.training).select(
                f.col(tenant_col).alias("df1_tenant"),
                f.col(user_col).alias("df1_user"),
                f.col(res_col).alias("df1_res"),
            ),
        )

        df2 = materialized_cache(
            without_ffa(data_set.training).select(
                f.col(tenant_col).alias("df2_tenant"),
                f.col(user_col).alias("df2_user"),
                f.col(res_col).alias("df2_res"),
            ),
        )

        df_joined_same_department = materialized_cache(
            df1.join(
                df2,
                (df1.df1_tenant == df2.df2_tenant)
                & (df1.df1_user != df2.df2_user)
                & (get_department(df1.df1_user) == get_department(df2.df2_user))
                & (df1.df1_res == df2.df2_res),
            )
            .groupBy("df1_tenant", df1.df1_user, df2.df2_user)
            .agg(f.count("*").alias("count")),
        )

        stats_same_map = create_stats(df_joined_same_department, "df1_tenant", "count")

        for stats_same in stats_same_map.get_stats():
            assert stats_same.count > 1
            assert 1 <= stats_same.min <= 2
            assert stats_same.max >= 9
            assert stats_same.mean >= 5

        df_joined_diff_department = materialized_cache(
            df1.join(
                df2,
                (df1.df1_tenant == df2.df2_tenant)
                & (df1.df1_user != df2.df2_user)
                & (get_department(df1.df1_user) != get_department(df2.df2_user))
                & (df1.df1_res == df2.df2_res),
            )
            .groupBy("df1_tenant", df1.df1_user, df2.df2_user)
            .agg(f.count("*").alias("count")),
        )

        assert df_joined_diff_department.count() == 0

        assert (
            without_ffa(data_set.intra_test)
            .filter(get_department(user_col) == get_department(res_col))
            .count()
            == without_ffa(data_set.intra_test).count()
        )

        assert (
            without_ffa(data_set.inter_test)
            .filter(get_department(user_col) != get_department(res_col))
            .count()
            == without_ffa(data_set.inter_test).count()
        )

    def report_cross_access(self, model: AccessAnomalyModel):
        training_scores = materialized_cache(model.transform(data_set.training))
        training_stats: StatsMap = create_stats(
            training_scores,
            AccessAnomalyConfig.default_tenant_col,
        )

        print("training_stats")

        for stats in training_stats.get_stats():
            assert abs(stats.mean) < epsilon
            assert abs(stats.std - 1.0) < epsilon
            print(stats)

        num_tenants = (
            data_set.training.select(AccessAnomalyConfig.default_tenant_col)
            .distinct()
            .count()
        )
        expected_num_components = num_tenants * (1 if single_component_data else 3)

        assert (
            model.user_res_feature_vector_mapping.user2component_mappings_df.select(
                "component",
            )
            .distinct()
            .count()
            == expected_num_components
        )

        assert (
            model.user_res_feature_vector_mapping.res2component_mappings_df.select(
                "component",
            )
            .distinct()
            .count()
            == expected_num_components
        )

        intra_test_scores = model.transform(data_set.intra_test)
        intra_test_stats = create_stats(
            intra_test_scores,
            AccessAnomalyConfig.default_tenant_col,
        )

        inter_test_scores = model.transform(data_set.inter_test)
        inter_test_stats = create_stats(
            inter_test_scores,
            AccessAnomalyConfig.default_tenant_col,
        )

        print("test_stats")

        for tid in inter_test_stats.get_tenants():
            intra_stats = intra_test_stats.get_stat(tid)
            inter_stats = inter_test_stats.get_stat(tid)

            assert inter_stats.mean > intra_stats.mean
            assert inter_stats.mean - intra_stats.mean >= 2.0

            print(tid)
            print(intra_stats)
            print(inter_stats)

    def test_cross_access(self):
        self.report_cross_access(data_set.get_default_access_anomaly_model())


class TestConnectedComponents:
    def test_simple(self):
        tenant_col = "tenant"
        user_col = "user"
        res_col = "res"
        likelihood_col = "likelihood"

        df_schema = t.StructType(
            [
                t.StructField(tenant_col, t.StringType(), False),
                t.StructField(user_col, t.StringType(), False),
                t.StructField(res_col, t.StringType(), False),
                t.StructField(likelihood_col, t.DoubleType(), False),
            ],
        )

        df = spark.createDataFrame(
            [
                ["0", "user0", "res0", 4.0],
                ["0", "user1", "res0", 8.0],
                ["0", "user1", "res1", 7.0],
                ["0", "user2", "res1", 8.0],
                ["0", "user3", "res1", 8.0],
                ["0", "user3", "res2", 8.0],
                ["0", "user4", "res3", 10.0],
            ],
            df_schema,
        )

        cc = ConnectedComponents(tenant_col, user_col, res_col)
        user2components, res2components = cc.transform(df)

        user_components = [
            row["users"]
            for row in user2components.groupBy("component")
            .agg(f.collect_list("user").alias("users"))
            .orderBy("component")
            .select("users")
            .collect()
        ]

        assert len(user_components) == 2
        assert set(user_components[0]) == {"user0", "user1", "user2", "user3"}
        assert set(user_components[1]) == {"user4"}

        res_components = [
            row["resources"]
            for row in res2components.groupBy("component")
            .agg(f.collect_list("res").alias("resources"))
            .orderBy("component")
            .select("resources")
            .collect()
        ]

        assert len(res_components) == 2
        assert set(res_components[0]) == {"res0", "res1", "res2"}
        assert set(res_components[1]) == {"res3"}

    def test_datafactory(self):
        tenant_col = "tenant"
        user_col = "user"
        res_col = "res"

        df = spark.createDataFrame(
            DataFactory(single_component=False).create_clustered_training_data(),
        ).withColumn(tenant_col, f.lit(0))

        cc = ConnectedComponents(tenant_col, user_col, res_col)
        user2components, res2components = cc.transform(df)

        assert user2components.select("component").distinct().count() == 3
        assert res2components.select("component").distinct().count() == 3
