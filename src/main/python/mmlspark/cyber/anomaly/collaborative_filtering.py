__author__ = 'rolevin'

import os
from typing import List, Optional, Tuple

from mmlspark.cyber.anomaly.complement_access import ComplementAccessTransformer
from mmlspark.cyber.feature import indexers, scalers
from mmlspark.cyber.utils import spark_utils

import numpy as np

from pyspark import SQLContext  # noqa
from pyspark.ml import Estimator, Transformer
from pyspark.ml.param.shared import Param, Params
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, functions as f, types as t

"""
Glossary:
the term 'res' in this package is a shorthand for resource
"""


def _make_dot():
    """
    create a method that performs a dot product between two vectors (list of doubles)
    :return: the method
    """
    @f.udf(t.DoubleType())
    def dot(v, u):
        if (v is not None) and (u is not None):
            vv = np.pad(np.array(v), (0, len(u) - len(v)), 'constant', constant_values=1.0) if len(v) < len(
                u) else np.array(v)
            uu = np.pad(np.array(u), (0, len(v) - len(u)), 'constant', constant_values=1.0) if len(u) < len(
                v) else np.array(u)

            return float(vv.dot(uu))
        else:
            return None

    return dot


class AccessAnomalyConfig:
    """
    Define default values for AccessAnomaly Params
    """
    default_tenant_col = 'tenant'
    default_user_col = 'user'
    default_res_col = 'res'
    default_likelihood_col = 'likelihood'
    default_output_col = 'anomaly_score'

    default_rank = 10
    default_max_iter = 25
    default_reg_param = 1.0
    default_num_blocks = None   # |tenants| if separate_tenants is False else 10
    default_separate_tenants = False

    default_low_value = 5.0
    default_high_value = 10.0

    default_apply_implicit_cf = True
    default_alpha = 1.0

    default_complementset_factor = 2
    default_neg_score = 1.0


class _UserResourceFeatureVectorMapping:
    """
    Private class used to pass the mappings as calculated by the AccessAnomaliesEstimator.
    An object representing the user and resource models
    (mapping from name to latent vector)
    and the relevant column names
    """
    def __init__(self,
                 tenant_col: str,
                 user_col: str,
                 user_vec_col: str,
                 res_col: str,
                 res_vec_col: str,
                 history_access_df: Optional[DataFrame],
                 user2component_mappings_df: Optional[DataFrame],
                 res2component_mappings_df: Optional[DataFrame],
                 user_feature_vector_mapping_df: DataFrame,
                 res_feature_vector_mapping_df: DataFrame):

        self.tenant_col = tenant_col
        self.user_col = user_col
        self.user_vec_col = user_vec_col
        self.res_col = res_col
        self.res_vec_col = res_vec_col
        self.history_access_df = history_access_df
        self.user2component_mappings_df = user2component_mappings_df
        self.res2component_mappings_df = res2component_mappings_df
        self.user_feature_vector_mapping_df = user_feature_vector_mapping_df
        self.res_feature_vector_mapping_df = res_feature_vector_mapping_df

        assert \
            self.history_access_df is None or \
            set(self.history_access_df.schema.fieldNames()) == {tenant_col, user_col, res_col}, \
            self.history_access_df.schema.fieldNames()

    def replace_mappings(
            self,
            user_feature_vector_mapping_df: Optional[DataFrame] = None,
            res_feature_vector_mapping_df: Optional[DataFrame] = None):

        """
        create a new model replacing the user and resource models with new ones (optional)

        :param user_feature_vector_mapping_df: optional new user model mapping names to latent vectors
        :param res_feature_vector_mapping_df: optional new resource model mapping names to latent vectors
        :return:
        """
        return _UserResourceFeatureVectorMapping(
            self.tenant_col,
            self.user_col,
            self.user_vec_col,
            self.res_col,
            self.res_vec_col,
            self.history_access_df,
            self.user2component_mappings_df,
            self.res2component_mappings_df,
            user_feature_vector_mapping_df
            if user_feature_vector_mapping_df is not None else self.user_feature_vector_mapping_df,
            res_feature_vector_mapping_df
            if res_feature_vector_mapping_df is not None else self.res_feature_vector_mapping_df
        )

    def check(self):
        """
        check the validity of the model
        :return: boolean value where True indicating the verification succeeded
        """
        return self._check_user_mapping() and self._check_res_mapping()

    def _check_user_mapping(self):
        field_map = {ff.name: ff for ff in self.user_feature_vector_mapping_df.schema.fields}

        assert field_map.get(self.tenant_col) is not None, field_map
        assert field_map.get(self.user_col) is not None

        return self.user_feature_vector_mapping_df.select(
            self.tenant_col, self.user_col
        ).distinct().count() == self.user_feature_vector_mapping_df.count()

    def _check_res_mapping(self):
        field_map = {ff.name: ff for ff in self.res_feature_vector_mapping_df.schema.fields}

        assert field_map.get(self.tenant_col) is not None, field_map
        assert field_map.get(self.res_col) is not None

        return self.res_feature_vector_mapping_df.select(
            self.tenant_col, self.res_col
        ).distinct().count() == self.res_feature_vector_mapping_df.count()


# noinspection PyPep8Naming
class AccessAnomalyModel(Transformer):
    outputCol = Param(
        Params._dummy(),
        "outputCol",
        "The name of the output column representing the calculated anomaly score. "
        "Values will be between (-inf, +inf) with an estimated mean of 0.0 and standard deviation of 1.0. "
    )

    """
    A pyspark.ml.Transformer model that can predict anomaly scores for user, resource access pairs
    """
    def __init__(self, userResourceFeatureVectorMapping: _UserResourceFeatureVectorMapping, outputCol: str):
        super().__init__()
        self.user_res_feature_vector_mapping = userResourceFeatureVectorMapping

        has_user2component_mappings = self.user_res_feature_vector_mapping.user2component_mappings_df is not None
        has_res2component_mappings = self.user_res_feature_vector_mapping.res2component_mappings_df is not None

        assert has_user2component_mappings == has_res2component_mappings

        self.has_components = has_user2component_mappings and has_res2component_mappings
        self.preserve_history = True

        if self.has_components:
            self._user_mapping_df = self.user_res_feature_vector_mapping.user_feature_vector_mapping_df.join(
                self.user_res_feature_vector_mapping.user2component_mappings_df, [self.tenant_col, self.user_col]
            ).select(
                self.tenant_col,
                self.user_col,
                self.user_vec_col,
                f.col('component').alias('user_component')
            ).cache()

            self._res_mapping_df = self.user_res_feature_vector_mapping.res_feature_vector_mapping_df.join(
                self.user_res_feature_vector_mapping.res2component_mappings_df, [self.tenant_col, self.res_col]
            ).select(
                self.tenant_col,
                self.res_col,
                self.res_vec_col,
                f.col('component').alias('res_component')
            ).cache()
        else:
            self._user_mapping_df = self.user_res_feature_vector_mapping.user_feature_vector_mapping_df
            self._res_mapping_df = self.user_res_feature_vector_mapping.res_feature_vector_mapping_df

        spark_utils.ExplainBuilder.build(self, outputCol=outputCol)

    @staticmethod
    def _metadata_schema() -> t.StructType:
        return t.StructType([
            t.StructField('tenant_col', t.StringType(), False),
            t.StructField('user_col', t.StringType(), False),
            t.StructField('user_vec_col', t.StringType(), False),
            t.StructField('res_col', t.StringType(), False),
            t.StructField('res_vec_col', t.StringType(), False),
            t.StructField('output_col', t.StringType(), False),

            t.StructField('has_history_access_df', t.BooleanType(), False),
            t.StructField('has_user2component_mappings_df', t.BooleanType(), False),
            t.StructField('has_res2component_mappings_df', t.BooleanType(), False),
            t.StructField('has_user_feature_vector_mapping_df', t.BooleanType(), False),
            t.StructField('has_res_feature_vector_mapping_df', t.BooleanType(), False)
        ])

    def save(self, path: str, path_suffix: str = '', output_format: str = 'parquet'):
        dfs = [
            self.user_res_feature_vector_mapping.history_access_df,
            self.user_res_feature_vector_mapping.user2component_mappings_df,
            self.user_res_feature_vector_mapping.res2component_mappings_df,
            self.user_res_feature_vector_mapping.user_feature_vector_mapping_df,
            self.user_res_feature_vector_mapping.res_feature_vector_mapping_df
        ]

        adf = next(iter([df for df in dfs if df is not None]))
        assert adf is not None

        spark = spark_utils.DataFrameUtils.get_spark_session(adf)

        metadata_df = spark.createDataFrame([
            (
                self.tenant_col,
                self.user_col,
                self.user_vec_col,
                self.res_col,
                self.res_vec_col,
                self.output_col,
                self.user_res_feature_vector_mapping.history_access_df is not None,
                self.user_res_feature_vector_mapping.user2component_mappings_df is not None,
                self.user_res_feature_vector_mapping.res2component_mappings_df is not None,
                self.user_res_feature_vector_mapping.user_feature_vector_mapping_df is not None,
                self.user_res_feature_vector_mapping.res_feature_vector_mapping_df is not None
            )
        ], AccessAnomalyModel._metadata_schema())

        metadata_df.write.format(output_format).save(os.path.join(path, 'metadata_df', path_suffix))

        if self.user_res_feature_vector_mapping.history_access_df is not None:
            self.user_res_feature_vector_mapping.history_access_df.write.format(output_format).save(
                os.path.join(path, 'history_access_df', path_suffix)
            )

        if self.user_res_feature_vector_mapping.user2component_mappings_df is not None:
            self.user_res_feature_vector_mapping.user2component_mappings_df.write.format(output_format).save(
                os.path.join(path, 'user2component_mappings_df', path_suffix)
            )

        if self.user_res_feature_vector_mapping.res2component_mappings_df is not None:
            self.user_res_feature_vector_mapping.res2component_mappings_df.write.format(output_format).save(
                os.path.join(path, 'res2component_mappings_df', path_suffix)
            )

        if self.user_res_feature_vector_mapping.user_feature_vector_mapping_df is not None:
            self.user_res_feature_vector_mapping.user_feature_vector_mapping_df.write.format(output_format).save(
                os.path.join(path, 'user_feature_vector_mapping_df', path_suffix)
            )

        if self.user_res_feature_vector_mapping.res_feature_vector_mapping_df is not None:
            self.user_res_feature_vector_mapping.res_feature_vector_mapping_df.write.format(output_format).save(
                os.path.join(path, 'res_feature_vector_mapping_df', path_suffix)
            )

    @staticmethod
    def load(spark: SQLContext, path: str, output_format: str = 'parquet') -> 'AccessAnomalyModel':
        metadata_df = spark.read.format(output_format).load(os.path.join(path, 'metadata_df'))
        assert metadata_df.count() == 1

        metadata_row = metadata_df.collect()[0]

        tenant_col = metadata_row['tenant_col']
        user_col = metadata_row['user_col']
        user_vec_col = metadata_row['user_vec_col']
        res_col = metadata_row['res_col']
        res_vec_col = metadata_row['res_vec_col']
        output_col = metadata_row['output_col']

        has_history_access_df = metadata_row['has_history_access_df']
        has_user2component_mappings_df = metadata_row['has_user2component_mappings_df']
        has_res2component_mappings_df = metadata_row['has_res2component_mappings_df']
        has_user_feature_vector_mapping_df = metadata_row['has_user_feature_vector_mapping_df']
        has_res_feature_vector_mapping_df = metadata_row['has_res_feature_vector_mapping_df']

        history_access_df = spark.read.format(output_format).load(
            os.path.join(path, 'history_access_df')
        ) if has_history_access_df else None

        user2component_mappings_df = spark.read.format(output_format).load(
            os.path.join(path, 'user2component_mappings_df')
        ) if has_user2component_mappings_df else None

        res2component_mappings_df = spark.read.format(output_format).load(
            os.path.join(path, 'res2component_mappings_df')
        ) if has_res2component_mappings_df else None

        user_feature_vector_mapping_df = spark.read.format(output_format).load(
            os.path.join(path, 'user_feature_vector_mapping_df')
        ) if has_user_feature_vector_mapping_df else None

        res_feature_vector_mapping_df = spark.read.format(output_format).load(
            os.path.join(path, 'res_feature_vector_mapping_df')
        ) if has_res_feature_vector_mapping_df else None

        return AccessAnomalyModel(
            _UserResourceFeatureVectorMapping(
                tenant_col,
                user_col,
                user_vec_col,
                res_col,
                res_vec_col,
                history_access_df,
                user2component_mappings_df,
                res2component_mappings_df,
                user_feature_vector_mapping_df,
                res_feature_vector_mapping_df
            ),
            output_col
        )

    @property
    def tenant_col(self):
        return self.user_res_feature_vector_mapping.tenant_col

    @property
    def user_col(self):
        return self.user_res_feature_vector_mapping.user_col

    @property
    def user_vec_col(self):
        return self.user_res_feature_vector_mapping.user_vec_col

    @property
    def res_col(self):
        return self.user_res_feature_vector_mapping.res_col

    @property
    def res_vec_col(self):
        return self.user_res_feature_vector_mapping.res_vec_col

    @property
    def user_mapping_df(self):
        return self._user_mapping_df

    @property
    def res_mapping_df(self):
        return self._res_mapping_df

    def _transform(self, df: DataFrame) -> DataFrame:
        dot = _make_dot()

        tenant_col = self.tenant_col
        user_col = self.user_col
        user_vec_col = self.user_vec_col
        res_col = self.res_col
        res_vec_col = self.res_vec_col
        output_col = self.output_col

        seen_token = '__seen__'

        def value_calc():
            return f.when(f.col(seen_token).isNull() | ~f.col(seen_token), f.when(
                f.col(user_vec_col).isNotNull() & f.col(res_vec_col).isNotNull(),
                f.when(
                    f.col('user_component') == f.col('res_component'),
                    dot(f.col(user_vec_col), f.col(res_vec_col))
                ).otherwise(f.lit(float("inf")))
            ).otherwise(f.lit(None)) if self.has_components else f.when(
                f.col(user_vec_col).isNotNull() & f.col(res_vec_col).isNotNull(),
                dot(f.col(user_vec_col), f.col(res_vec_col))
            ).otherwise(f.lit(None))).otherwise(f.lit(0.0))

        history_access_df = self.user_res_feature_vector_mapping.history_access_df

        the_df = df.join(
            history_access_df.withColumn(seen_token, f.lit(True)),
            [tenant_col, user_col, res_col],
            how='left'
        ) if self.preserve_history and history_access_df is not None else df.withColumn(seen_token, f.lit(False))

        user_mapping_df = self.user_mapping_df
        res_mapping_df = self.res_mapping_df

        return the_df.join(
            user_mapping_df, [tenant_col, user_col], how='left'
        ).join(
            res_mapping_df, [tenant_col, res_col], how='left'
        ).withColumn(output_col, value_calc()).drop(
            user_vec_col,
            res_vec_col,
            'user_component',
            'res_component',
            seen_token
        )


# noinspection PyPep8Naming
class ConnectedComponents:
    def __init__(self, tenantCol: str, userCol: str, res_col: str, componentColName: str = 'component'):
        self.tenant_col = tenantCol
        self.user_col = userCol
        self.res_col = res_col
        self.component_col_name = componentColName

    def transform(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        edges = df.select(self.tenant_col, self.user_col, self.res_col).distinct().orderBy(
            self.tenant_col, self.user_col, self.res_col
        ).cache()

        users = df.select(self.tenant_col, self.user_col).distinct().orderBy(self.tenant_col, self.user_col).cache()
        user2index = spark_utils.DataFrameUtils.zip_with_index(users, col_name='user_component')
        user2components = user2index
        res2components = None

        chg = True

        while chg:
            res2components = edges.join(
                user2components, [self.tenant_col, self.user_col]
            ).groupBy(self.tenant_col, self.res_col).agg(
                f.min('user_component').alias('res_component')
            )

            next_user2components = edges.join(
                res2components, [self.tenant_col, self.res_col]
            ).groupBy(self.tenant_col, self.user_col).agg(
                f.min('res_component').alias('user_component')
            ).cache()

            chg = user2components.join(
                next_user2components, on=[self.tenant_col, self.user_col, 'user_component']
            ).count() != user2components.count()

            user2components = next_user2components

        assert res2components is not None

        return (
            user2components.select(
                self.tenant_col, self.user_col, f.col('user_component').alias(self.component_col_name)
            ).orderBy(
                self.tenant_col,
                self.user_col
            ),
            res2components.select(
                self.tenant_col, self.res_col, f.col('res_component').alias(self.component_col_name)
            ).orderBy(
                self.tenant_col,
                self.res_col
            )
        )


# noinspection PyPep8Naming
class AccessAnomaly(Estimator):
    """
    This is the AccessAnomaly, a pyspark.ml.Estimator which
    creates the AccessAnomalyModel which is a pyspark.ml.Transformer
    """
    tenantCol = Param(
        Params._dummy(),
        "tenantCol",
        "The name of the tenant column. "
        "This is a unique identifier used to partition the dataframe into independent "
        "groups where the values in each such group are completely isolated from one another. "
        "Note: if this column is irrelevant for your data, "
        "then just create a tenant column and give it a single value for all rows."
    )

    userCol = Param(
        Params._dummy(),
        "userCol",
        "The name of the user column. "
        "This is a the name of the user column in the dataframe."
    )

    resCol = Param(
        Params._dummy(),
        "resCol",
        "The name of the resource column. "
        "This is a the name of the resource column in the dataframe."
    )

    likelihoodCol = Param(
        Params._dummy(),
        "likelihoodCol",
        "The name of the column with the likelihood estimate for user, res access "
        "(usually based on access counts per time unit). "
    )

    outputCol = Param(
        Params._dummy(),
        "outputCol",
        "The name of the output column representing the calculated anomaly score. "
        "Values will be between (-inf, +inf) with an estimated mean of 0.0 and standard deviation of 1.0. "
    )

    rankParam = Param(
        Params._dummy(),
        "rankParam",
        "rankParam is the number of latent factors in the model (defaults to 10)."
    )

    maxIter = Param(
        Params._dummy(),
        "maxIter",
        "maxIter is the maximum number of iterations to run (defaults to 25)."
    )

    regParam = Param(
        Params._dummy(),
        "regParam",
        "regParam specifies the regularization parameter in ALS (defaults to 0.1)."
    )

    numBlocks = Param(
        Params._dummy(),
        "numBlocks",
        "numBlocks is the number of blocks the users and items will be partitioned into "
        "in order to parallelize computation "
        "(defaults to |tenants| if separate_tenants is False else 10)."
    )

    separateTenants = Param(
        Params._dummy(),
        "separateTenants",
        "separateTenants applies the algorithm per tenant in isolation. "
        "Setting to True may reduce runtime significantly, if number of tenant is large, "
        "but will increase accuracy. (defaults to False)."
    )

    lowValue = Param(
        Params._dummy(),
        "lowValue",
        "lowValue is used to scale the values of likelihood_col to be in the range [lowValue, highValue] "
        "(defaults to 5.0)."
    )

    highValue = Param(
        Params._dummy(),
        "highValue",
        "highValue is used to scale the values of likelihood_col to be in the range [lowValue, highValue] "
        "(defaults to 10.0)."
    )

    applyImplicitCf = Param(
        Params._dummy(),
        "applyImplicitCf",
        "specifies whether to use the implicit/explicit feedback ALS for the data "
        "(defaults to True which means using implicit feedback)."
    )

    alphaParam = Param(
        Params._dummy(),
        "alphaParam",
        "alphaParam is a parameter applicable to the implicit feedback variant "
        "of ALS that governs the baseline confidence in preference observations."
        "(defaults to 1.0)."
    )

    complementsetFactor = Param(
        Params._dummy(),
        "complementsetFactor",
        "complementsetFactor is a parameter applicable to the implicit feedback variant "
        "of ALS that governs the baseline confidence in preference observations."
        "(defaults to 2)."
    )

    negScore = Param(
        Params._dummy(),
        "negScore",
        "negScore is a parameter applicable to the explicit feedback variant of ALS that governs "
        "the value to assign to the values of the complement set."
        "(defaults to 1.0)."
    )

    historyAccessDf = Param(
        Params._dummy(),
        "historyAccessDf",
        "historyAccessDf is an optional spark dataframe which includes the "
        "list of seen user resource pairs for which the anomaly score should be zero."
    )

    def __init__(self,
                 tenantCol: str = AccessAnomalyConfig.default_tenant_col,
                 userCol: str = AccessAnomalyConfig.default_user_col,
                 resCol: str = AccessAnomalyConfig.default_res_col,
                 likelihoodCol: str = AccessAnomalyConfig.default_likelihood_col,
                 outputCol: str = AccessAnomalyConfig.default_output_col,
                 rankParam: int = AccessAnomalyConfig.default_rank,
                 maxIter: int = AccessAnomalyConfig.default_max_iter,
                 regParam: float = AccessAnomalyConfig.default_reg_param,
                 numBlocks: Optional[int] = AccessAnomalyConfig.default_num_blocks,
                 separateTenants: bool = AccessAnomalyConfig.default_separate_tenants,
                 lowValue: Optional[float] = AccessAnomalyConfig.default_low_value,
                 highValue: Optional[float] = AccessAnomalyConfig.default_high_value,
                 applyImplicitCf: bool = AccessAnomalyConfig.default_apply_implicit_cf,
                 alphaParam: Optional[float] = None,
                 complementsetFactor: Optional[int] = None,
                 negScore: Optional[float] = None,
                 historyAccessDf: Optional[DataFrame] = None):

        super().__init__()

        if applyImplicitCf:
            alphaParam = alphaParam if alphaParam is not None else AccessAnomalyConfig.default_alpha
            assert complementsetFactor is None and negScore is None
        else:
            assert alphaParam is None

            complementsetFactor = \
                complementsetFactor if complementsetFactor is not None else AccessAnomalyConfig.default_complementset_factor

            negScore = negScore \
                if negScore is not None else AccessAnomalyConfig.default_neg_score

        # must either both be None or both be not None
        assert (lowValue is None) == (highValue is None)
        assert lowValue is None or lowValue >= 1.0
        assert (lowValue is None or highValue is None) or highValue > lowValue
        assert \
            (lowValue is None or negScore is None) or \
            (lowValue is not None and negScore < lowValue)

        spark_utils.ExplainBuilder.build(
            self,
            tenantCol=tenantCol,
            userCol=userCol,
            resCol=resCol,
            likelihoodCol=likelihoodCol,
            outputCol=outputCol,
            rankParam=rankParam,
            maxIter=maxIter,
            regParam=regParam,
            numBlocks=numBlocks,
            separateTenants=separateTenants,
            lowValue=lowValue,
            highValue=highValue,
            applyImplicitCf=applyImplicitCf,
            alphaParam=alphaParam,
            complementsetFactor=complementsetFactor,
            negScore=negScore,
            historyAccessDf=historyAccessDf
        )

    # --- getters and setters
    @property
    def indexed_user_col(self):
        return self.user_col + '_index'

    @property
    def user_vec_col(self):
        return self.user_col + '_vector'

    @property
    def indexed_res_col(self):
        return self.res_col + '_index'

    @property
    def res_vec_col(self):
        return self.res_col + '_vector'

    @property
    def scaled_likelihood_col(self):
        return self.likelihood_col + '_scaled'

    def _get_scaled_df(self, df: DataFrame) -> DataFrame:
        return scalers.LinearScalarScaler(
            input_col=self.likelihood_col,
            partition_key=self.tenant_col,
            output_col=self.scaled_likelihood_col,
            min_required_value=self.low_value,
            max_required_value=self.high_value
        ).fit(df).transform(df) if self.low_value is not None and self.high_value is not None else df

    def _enrich_and_normalize(self, indexed_df: DataFrame) -> DataFrame:
        tenant_col = self.tenant_col
        indexed_user_col = self.indexed_user_col
        indexed_res_col = self.indexed_res_col
        scaled_likelihood_col = self.scaled_likelihood_col

        if not self.apply_implicit_cf:
            complementset_factor = self.complementset_factor
            neg_score = self.neg_score
            assert complementset_factor is not None and neg_score is not None

            comp_df = ComplementAccessTransformer(
                tenant_col, [indexed_user_col, indexed_res_col], complementset_factor
            ).transform(indexed_df).withColumn(
                scaled_likelihood_col,
                f.lit(neg_score)
            )
        else:
            comp_df = None

        scaled_df = self._get_scaled_df(indexed_df).select(
            tenant_col, indexed_user_col, indexed_res_col, scaled_likelihood_col
        )

        return scaled_df.union(comp_df) if comp_df is not None else scaled_df

    def _train_cf(self, als: ALS, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        tenant_col = self.tenant_col
        indexed_user_col = self.indexed_user_col
        user_vec_col = self.user_vec_col
        indexed_res_col = self.indexed_res_col
        res_vec_col = self.res_vec_col

        spark_model = als.fit(df)

        user_mapping_df = spark_model.userFactors.select(
            f.col('id').alias(indexed_user_col),
            f.col('features').alias(user_vec_col)
        ).join(
            df.select(indexed_user_col, tenant_col).distinct(), indexed_user_col
        ).select(
            tenant_col, indexed_user_col, user_vec_col
        )

        res_mapping_df = spark_model.itemFactors.select(
            f.col('id').alias(indexed_res_col),
            f.col('features').alias(res_vec_col)
        ).join(
            df.select(indexed_res_col, tenant_col).distinct(), indexed_res_col
        ).select(
            tenant_col, indexed_res_col, res_vec_col
        )

        return user_mapping_df, res_mapping_df

    def create_spark_model_vectors_df(self, df: DataFrame) -> _UserResourceFeatureVectorMapping:
        tenant_col = self.tenant_col
        indexed_user_col = self.indexed_user_col
        user_vec_col = self.user_vec_col
        indexed_res_col = self.indexed_res_col
        res_vec_col = self.res_vec_col
        max_iter = self.max_iter
        distinct_tenants = df.select(tenant_col).distinct().cache()
        num_tenants = distinct_tenants.count()
        separate_tenants = self.separate_tenants
        num_blocks = self.num_blocks if self.num_blocks is not None else (num_tenants if not separate_tenants else 10)

        als = ALS(
            rank=self.rank_param,
            maxIter=max_iter,
            regParam=self.reg_param,
            numUserBlocks=num_blocks,
            numItemBlocks=num_blocks,
            implicitPrefs=self.apply_implicit_cf,
            userCol=self.indexed_user_col,
            itemCol=self.indexed_res_col,
            ratingCol=self.scaled_likelihood_col,
            nonnegative=True,
            coldStartStrategy='drop'
        )

        alpha = self.alpha_param

        if alpha is not None:
            als.setAlpha(alpha)

        if separate_tenants:
            tenants = [row[tenant_col] for row in distinct_tenants.orderBy(tenant_col).collect()]

            user_mapping_df: Optional[DataFrame] = None
            res_mapping_df: Optional[DataFrame] = None

            for curr_tenant in tenants:
                curr_df = df.filter(f.col(tenant_col) == curr_tenant).cache()
                curr_user_mapping_df, curr_res_mapping_df = self._train_cf(als, curr_df)

                user_mapping_df = user_mapping_df.union(
                    curr_user_mapping_df
                ) if user_mapping_df is not None else curr_user_mapping_df

                res_mapping_df = res_mapping_df.union(
                    curr_res_mapping_df
                ) if res_mapping_df is not None else curr_res_mapping_df
        else:
            user_mapping_df, res_mapping_df = self._train_cf(als, df)

        assert user_mapping_df is not None and res_mapping_df is not None

        return _UserResourceFeatureVectorMapping(
            tenant_col,
            indexed_user_col,
            user_vec_col,
            indexed_res_col,
            res_vec_col,
            None,
            None,
            None,
            user_mapping_df,
            res_mapping_df
        )

    def _fit(self, df: DataFrame) -> AccessAnomalyModel:
        # index the user and resource columns to allow running the spark ALS algorithm
        the_indexer = indexers.MultiIndexer(
            indexers=[
                indexers.IdIndexer(
                    input_col=self.user_col,
                    partition_key=self.tenant_col,
                    output_col=self.indexed_user_col,
                    reset_per_partition=self.separate_tenants
                ),
                indexers.IdIndexer(
                    input_col=self.res_col,
                    partition_key=self.tenant_col,
                    output_col=self.indexed_res_col,
                    reset_per_partition=self.separate_tenants
                )
            ]
        )

        the_indexer_model = the_indexer.fit(df)

        # indexed_df is the dataframe with the indices for user and resource
        indexed_df = the_indexer_model.transform(df)
        enriched_df = self._enrich_and_normalize(indexed_df).cache()

        user_res_feature_vector_mapping_df = self.create_spark_model_vectors_df(enriched_df)
        user_res_norm_cf_df_model = ModelNormalizeTransformer(
            enriched_df, self.rank_param
        ).transform(user_res_feature_vector_mapping_df)

        # convert user and resource indices back to names
        user_index_model = the_indexer_model.get_model_by_input_col(self.user_col)
        res_index_model = the_indexer_model.get_model_by_input_col(self.res_col)
        assert user_index_model is not None and res_index_model is not None

        norm_user_mapping_df = user_res_norm_cf_df_model.user_feature_vector_mapping_df
        norm_res_mapping_df = user_res_norm_cf_df_model.res_feature_vector_mapping_df

        indexed_user_col = self.indexed_user_col
        indexed_res_col = self.indexed_res_col

        # do the actual index to name mapping (using undo_transform)
        final_user_mapping_df = user_index_model.undo_transform(norm_user_mapping_df).drop(indexed_user_col)
        final_res_mapping_df = res_index_model.undo_transform(norm_res_mapping_df).drop(indexed_res_col)

        tenant_col, user_col, res_col = self.tenant_col, self.user_col, self.res_col

        history_access_df = self.history_access_df
        access_df = \
            history_access_df if history_access_df is not None else df.select(tenant_col, user_col, res_col).cache()

        user2component_mappings_df, res2component_mappings_df = ConnectedComponents(
            tenant_col, user_col, res_col
        ).transform(access_df)

        return AccessAnomalyModel(
            _UserResourceFeatureVectorMapping(
                tenant_col=self.tenant_col,
                user_col=self.user_col,
                user_vec_col=self.user_vec_col,
                res_col=self.res_col,
                res_vec_col=self.res_vec_col,
                history_access_df=history_access_df,
                user2component_mappings_df=user2component_mappings_df,
                res2component_mappings_df=res2component_mappings_df,
                user_feature_vector_mapping_df=final_user_mapping_df.cache(),
                res_feature_vector_mapping_df=final_res_mapping_df.cache()
            ),
            self.output_col
        )


class ModelNormalizeTransformer:
    """
    Given a UserResourceCfDataframeModel this class creates and returns
    a new normalized UserResourceCfDataframeModel which has an anomaly score
    with a mean of 0.0 and standard deviation of 1.0 when applied on the given dataframe
    """
    def __init__(self, access_df: DataFrame, rank: int):
        self.access_df = access_df
        self.rank = rank

    def _make_append_bias(self, user_col: str, res_col: str, col_name: str, target_col_name: str, rank: int) -> f.udf:
        assert col_name == user_col or col_name == res_col
        assert target_col_name == user_col or target_col_name == res_col

        def value_at(bias: float, value: float, i: int) -> float:
            if col_name != target_col_name or i < rank:
                res = value
            elif col_name == user_col and i == rank:
                res = bias + value
            elif col_name == res_col and i == (rank + 1):
                res = bias + value
            else:
                res = value

            assert res == 1.0 if i == rank and col_name == res_col else True
            assert res == 1.0 if i == (rank + 1) and col_name == user_col else True

            return res

        @f.udf(t.ArrayType(t.DoubleType()))
        def append_bias(v: List[float], bias: float, coeff: float = 1.0) -> List[float]:
            assert len(v) == rank or len(v) == rank + 2

            if len(v) == rank:
                # increase vector size to adjust for bias
                fix_value = bias if col_name == target_col_name else 0.0
                u = [fix_value, 1.0] if col_name == user_col else [1.0, fix_value]
                return [float(coeff * value) for value in np.append(np.array(v), np.array(u))]
            else:
                # fix enhanced vector to adjust for another bias
                assert len(v) == rank + 2
                return [coeff * value_at(bias, v[i], i) for i in range(len(v))]

        return append_bias

    def transform(self, user_res_cf_df_model: _UserResourceFeatureVectorMapping) -> _UserResourceFeatureVectorMapping:
        likelihood_col_token = '__likelihood__'

        dot = _make_dot()

        tenant_col = user_res_cf_df_model.tenant_col
        user_col = user_res_cf_df_model.user_col
        user_vec_col = user_res_cf_df_model.user_vec_col
        res_col = user_res_cf_df_model.res_col
        res_vec_col = user_res_cf_df_model.res_vec_col

        fixed_df = self.access_df.join(
            user_res_cf_df_model.user_feature_vector_mapping_df, [tenant_col, user_col]
        ).join(
            user_res_cf_df_model.res_feature_vector_mapping_df, [tenant_col, res_col]
        ).select(
            tenant_col,
            user_col,
            user_vec_col,
            res_col,
            res_vec_col,
            dot(f.col(user_vec_col), f.col(res_vec_col)).alias(likelihood_col_token)
        )

        scaler_model = scalers.StandardScalarScaler(
            likelihood_col_token, tenant_col, user_vec_col
        ).fit(fixed_df)

        per_group_stats: DataFrame = scaler_model.per_group_stats
        assert isinstance(per_group_stats, DataFrame)

        append2user_bias = self._make_append_bias(user_col, res_col, user_col, user_col, self.rank)
        append2res_bias = self._make_append_bias(user_col, res_col, res_col, user_col, self.rank)

        fixed_user_mapping_df = user_res_cf_df_model.user_feature_vector_mapping_df.join(
            per_group_stats, tenant_col
        ).select(
            tenant_col,
            user_col,
            append2user_bias(
                f.col(user_vec_col),
                f.lit(-1.0) * f.col(scalers.StandardScalarScalerConfig.mean_token),
                f.lit(-1.0) / f.when(
                    f.col(scalers.StandardScalarScalerConfig.std_token) != 0.0,
                    f.col(scalers.StandardScalarScalerConfig.std_token)
                ).otherwise(f.lit(1.0))
            ).alias(user_vec_col)
        )

        fixed_res_mapping_df = user_res_cf_df_model.res_feature_vector_mapping_df.join(
            per_group_stats, tenant_col
        ).select(
            tenant_col,
            res_col,
            append2res_bias(f.col(res_vec_col), f.lit(0)).alias(res_vec_col)
        )

        return user_res_cf_df_model.replace_mappings(fixed_user_mapping_df, fixed_res_mapping_df)
