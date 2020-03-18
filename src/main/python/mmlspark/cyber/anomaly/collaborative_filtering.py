__author__ = 'rolevin'

from typing import List, Optional, Tuple

from mmlspark.cyber.anomaly.complement_access import ComplementAccessTransformer
from mmlspark.cyber.feature import *
from mmlspark.cyber.utils import spark_utils
from mmlspark.cyber.utils.common import timefunc

import numpy as np

from pyspark.ml import Estimator, Transformer
from pyspark.ml.param.shared import Param, Params
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, functions as f, types as t
from pyspark.ml import Pipeline

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


class UserResourceCfDataframeModel:
    """
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
                 user_model_df: DataFrame,
                 res_model_df: DataFrame):
        self.tenant_col = tenant_col
        self.user_col = user_col
        self.user_vec_col = user_vec_col
        self.res_col = res_col
        self.res_vec_col = res_vec_col
        self.user_model_df = user_model_df
        self.res_model_df = res_model_df

    def replace_models(self, user_model_df: Optional[DataFrame] = None, res_model_df: Optional[DataFrame] = None):
        """
        create a new model replacing the user and resource models with new ones (optional)

        :param user_model_df: optional new user model mapping names to latent vectors
        :param res_model_df: optional new resource model mapping names to latent vectors
        :return:
        """
        return UserResourceCfDataframeModel(
            self.tenant_col,
            self.user_col,
            self.user_vec_col,
            self.res_col,
            self.res_vec_col,
            user_model_df if user_model_df is not None else self.user_model_df,
            res_model_df if res_model_df is not None else self.res_model_df
        )

    def check(self):
        """
        check the validity of the model
        :return: boolean value where True indicating the verification succeeded
        """
        return self._check_user_model() and self._check_res_model()

    def _check_user_model(self):
        field_map = {ff.name: ff for ff in self.user_model_df.schema.fields}

        assert field_map.get(self.tenant_col) is not None, field_map
        assert field_map.get(self.user_col) is not None

        return self.user_model_df.select(
            self.tenant_col, self.user_col
        ).distinct().count() == self.user_model_df.count()

    def _check_res_model(self):
        field_map = {ff.name: ff for ff in self.res_model_df.schema.fields}

        assert field_map.get(self.tenant_col) is not None, field_map
        assert field_map.get(self.res_col) is not None

        return self.res_model_df.select(
            self.tenant_col, self.res_col
        ).distinct().count() == self.res_model_df.count()


class AccessAnomalyModel(Transformer):
    """
    A pyspark.ml.Transformer model that can predict anomaly scores for user, resource access pairs
    """

    def __init__(self, user_res_cf_df_model: UserResourceCfDataframeModel, output_col: str):
        self.user_res_cf_df_model = user_res_cf_df_model
        self.output_col = output_col

    @property
    def tenant_col(self):
        return self.user_res_cf_df_model.tenant_col

    @property
    def user_col(self):
        return self.user_res_cf_df_model.user_col

    @property
    def user_vec_col(self):
        return self.user_res_cf_df_model.user_vec_col

    @property
    def res_col(self):
        return self.user_res_cf_df_model.res_col

    @property
    def res_vec_col(self):
        return self.user_res_cf_df_model.res_vec_col

    @property
    def user_model_df(self):
        return self.user_res_cf_df_model.user_model_df

    @property
    def res_model_df(self):
        return self.user_res_cf_df_model.res_model_df

    def _transform(self, df: DataFrame) -> DataFrame:
        dot = _make_dot()

        tenant_col = self.tenant_col
        user_col = self.user_col
        user_vec_col = self.user_vec_col
        res_col = self.res_col
        res_vec_col = self.res_vec_col
        output_col = self.output_col

        return df.join(
            self.user_model_df, [tenant_col, user_col], how='left'
        ).join(
            self.res_model_df, [tenant_col, res_col], how='left'
        ).withColumn(
            output_col,
            dot(f.col(user_vec_col), f.col(res_vec_col))
        ).drop(
            user_vec_col
        ).drop(
            res_vec_col
        )


class CfAlgoParams:
    """
    Parameter for the AccessAnomaly Estimator indicating
    if to use implicit or explicit feedback versions of the ALS algorithm
    and additional parameters that are relevant for the implicit/explicit versions
    """

    def __init__(self, implicit: bool):
        self._implicit = implicit
        self._alpha: Optional[float] = None
        self._complementset_factor: Optional[int] = None
        self._neg_score: Optional[float] = None

        # set default values
        if implicit:
            self.set_alpha(1.0)
        else:
            self.set_complementset_factor(2)
            self.set_neg_score(1.0)

    @property
    def implicit(self) -> bool:
        return self._implicit

    @property
    def alpha(self) -> Optional[float]:
        """
        Relevant for the implicit version only
        :return: alpha value (see descriptions in explainParam of AccessAnomaly)
        """
        return self._alpha

    @property
    def complementset_factor(self) -> Optional[int]:
        """
        Relevant for the explicit version only
        :return: complementset_factor value (see descriptions in explainParam of AccessAnomaly)
        """
        return self._complementset_factor

    @property
    def neg_score(self) -> Optional[float]:
        """
        Relevant for the explicit version only
        :return: neg_score value (see descriptions in explainParam of AccessAnomaly)
        """
        return self._neg_score

    def set_alpha(self, value: float):
        assert self.implicit
        self._alpha = value
        return self

    def set_complementset_factor(self, value: int):
        assert not self.implicit
        self._complementset_factor = value
        return self

    def set_neg_score(self, value: float):
        assert not self.implicit
        assert value > 0.0
        self._neg_score = value
        return self


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
    default_num_blocks = None  # |tenants| if separate_tenants is False else 10
    default_separate_tenants = False

    default_low_value = 5.0
    default_high_value = 10.0

    default_algo_cf_params = CfAlgoParams(True)


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

    algoCfParams = Param(
        Params._dummy(),
        "algoCfParams",
        "algoCfParams is comprised of the following items: "
        "'implicit' specifies whether to use the explicit feedback ALS variant or one adapted "
        "for implicit feedback data (defaults to false which means using explicit feedback). "
        "'alpha' is a parameter applicable to the implicit feedback variant of ALS that governs "
        "the baseline confidence in preference observations (defaults to 1.0). "
        "'complementset_factor' is a parameter applicable to the explicit feedback variant of ALS that governs "
        "that is used to generate a sample from the complement set of "
        "(user, res) access patterns seen in the training data. "
        "For example, a value of 2 indicates that the complement set should be an "
        "order of twice the size of the distinct (user, res) pairs in the training. (defaults to 2)."
        "'neg_score' is a parameter applicable to the explicit feedback variant of ALS that governs "
        "the value to assign to the values of the complement set. (defaults to 1.0)."
    )

    def __init__(self,
                 tenant_col: str = AccessAnomalyConfig.default_tenant_col,
                 user_col: str = AccessAnomalyConfig.default_user_col,
                 res_col: str = AccessAnomalyConfig.default_res_col,
                 likelihood_col: str = AccessAnomalyConfig.default_likelihood_col,
                 output_col: str = AccessAnomalyConfig.default_output_col,
                 rank_param: int = AccessAnomalyConfig.default_rank,
                 max_iter: int = AccessAnomalyConfig.default_max_iter,
                 reg_param: float = AccessAnomalyConfig.default_reg_param,
                 num_blocks: Optional[int] = AccessAnomalyConfig.default_num_blocks,
                 separate_tenants: bool = AccessAnomalyConfig.default_separate_tenants,
                 low_value: Optional[float] = AccessAnomalyConfig.default_low_value,
                 high_value: Optional[float] = AccessAnomalyConfig.default_high_value,
                 algo_cf_params: CfAlgoParams = AccessAnomalyConfig.default_algo_cf_params):

        super().__init__()

        # must either both be None or both be not None
        assert (low_value is None) == (high_value is None)
        assert low_value is None or low_value >= 1.0
        assert (low_value is None or high_value is None) or high_value > low_value
        assert (low_value is None or algo_cf_params.neg_score is None) or algo_cf_params.neg_score < low_value

        spark_utils.ExplainBuilder.build(
            self,
            tenantCol=tenant_col,
            userCol=user_col,
            resCol=res_col,
            likelihoodCol=likelihood_col,
            outputCol=output_col,
            rankParam=rank_param,
            maxIter=max_iter,
            regParam=reg_param,
            numBlocks=num_blocks,
            separateTenants=separate_tenants,
            lowValue=low_value,
            highValue=high_value,
            algoCfParams=algo_cf_params
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

    @timefunc
    def _get_scaled_df(self, df: DataFrame) -> DataFrame:
        return PartitionedMinMaxScaler(
            inputCol=self.likelihood_col,
            partitionKey=self.tenant_col,
            outputCol=self.scaled_likelihood_col,
            minValue=self.low_value,
            maxValue=self.high_value
        ).fit(df).transform(df) if self.low_value is not None and self.high_value is not None else df

    @timefunc
    def _enrich_and_normalize(self, indexed_df: DataFrame) -> DataFrame:
        tenant_col = self.tenant_col
        indexed_user_col = self.indexed_user_col
        indexed_res_col = self.indexed_res_col
        scaled_likelihood_col = self.scaled_likelihood_col

        if not self.algo_cf_params.implicit:
            complementset_factor = self.algo_cf_params.complementset_factor
            neg_score = self.algo_cf_params.neg_score
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

    @timefunc
    def _train_cf(self, als: ALS, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        tenant_col = self.tenant_col
        indexed_user_col = self.indexed_user_col
        user_vec_col = self.user_vec_col
        indexed_res_col = self.indexed_res_col
        res_vec_col = self.res_vec_col

        spark_model = als.fit(df)

        user_model_df = spark_model.userFactors.select(
            f.col('id').alias(indexed_user_col),
            f.col('features').alias(user_vec_col)
        ).join(
            df.select(indexed_user_col, tenant_col).distinct(), indexed_user_col
        ).select(
            tenant_col, indexed_user_col, user_vec_col
        )

        res_model_df = spark_model.itemFactors.select(
            f.col('id').alias(indexed_res_col),
            f.col('features').alias(res_vec_col)
        ).join(
            df.select(indexed_res_col, tenant_col).distinct(), indexed_res_col
        ).select(
            tenant_col, indexed_res_col, res_vec_col
        )

        return user_model_df, res_model_df

    @timefunc
    def create_spark_model_vectors_df(self, df: DataFrame) -> UserResourceCfDataframeModel:
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
            implicitPrefs=self.algo_cf_params.implicit,
            userCol=self.indexed_user_col,
            itemCol=self.indexed_res_col,
            ratingCol=self.scaled_likelihood_col,
            nonnegative=True,
            coldStartStrategy='drop'
        )

        alpha = self.algo_cf_params.alpha

        if alpha is not None:
            als.setAlpha(alpha)

        if separate_tenants:
            tenants = [row[tenant_col] for row in distinct_tenants.orderBy(tenant_col).collect()]

            user_model_df: Optional[DataFrame] = None
            res_model_df: Optional[DataFrame] = None

            for curr_tenant in tenants:
                curr_df = df.filter(f.col(tenant_col) == curr_tenant).cache()
                curr_user_model_df, curr_res_model_df = self._train_cf(als, curr_df)

                user_model_df = user_model_df.union(
                    curr_user_model_df
                ) if user_model_df is not None else curr_user_model_df

                res_model_df = res_model_df.union(
                    curr_res_model_df
                ) if res_model_df is not None else curr_res_model_df
        else:
            user_model_df, res_model_df = self._train_cf(als, df)

        assert user_model_df is not None and res_model_df is not None

        return UserResourceCfDataframeModel(
            tenant_col,
            indexed_user_col,
            user_vec_col,
            indexed_res_col,
            res_vec_col,
            user_model_df,
            res_model_df
        )

    @timefunc
    def _fit(self, df: DataFrame) -> AccessAnomalyModel:
        # index the user and resource columns to allow running the spark ALS algorithm
        indexer = Pipeline(stages=[
            PartitionedStringIndexer(
                inputCol=self.user_col,
                partitionKey=self.tenant_col,
                outputCol=self.indexed_user_col,
                overlappingIndicies=not self.separate_tenants
            ),
            PartitionedStringIndexer(
                inputCol=self.res_col,
                partitionKey=self.tenant_col,
                outputCol=self.indexed_res_col,
                overlappingIndicies=not self.separate_tenants
            )
        ]
        )

        indexer_model = indexer.fit(df)

        # indexed_df is the dataframe with the indices for user and resource
        indexed_df = indexer_model.transform(df)
        enriched_df = self._enrich_and_normalize(indexed_df).cache()

        user_res_cf_df_model = self.create_spark_model_vectors_df(enriched_df)
        user_res_norm_cf_df_model = ModelNormalizeTransformer(
            enriched_df, self.rank_param
        ).transform(user_res_cf_df_model)

        # convert user and resource indices back to names
        user_index_model = indexer_model.stages[0]
        res_index_model = indexer_model.stages[1]
        assert user_index_model is not None and res_index_model is not None

        norm_user_model_df = user_res_norm_cf_df_model.user_model_df
        norm_res_model_df = user_res_norm_cf_df_model.res_model_df

        indexed_user_col = self.indexed_user_col
        indexed_res_col = self.indexed_res_col

        # do the actual index to name mapping (using undo_transform)
        final_user_model_df = user_index_model.undo_transform(norm_user_model_df).drop(indexed_user_col)
        final_res_model_df = res_index_model.undo_transform(norm_res_model_df).drop(indexed_res_col)

        return AccessAnomalyModel(
            UserResourceCfDataframeModel(
                tenant_col=self.tenant_col,
                user_col=self.user_col,
                user_vec_col=self.user_vec_col,
                res_col=self.res_col,
                res_vec_col=self.res_vec_col,
                user_model_df=final_user_model_df.cache(),
                res_model_df=final_res_model_df.cache()
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

    @timefunc
    def transform(self, user_res_cf_df_model: UserResourceCfDataframeModel) -> UserResourceCfDataframeModel:
        likelihood_col_token = '__likelihood__'

        dot = _make_dot()

        tenant_col = user_res_cf_df_model.tenant_col
        user_col = user_res_cf_df_model.user_col
        user_vec_col = user_res_cf_df_model.user_vec_col
        res_col = user_res_cf_df_model.res_col
        res_vec_col = user_res_cf_df_model.res_vec_col

        fixed_df = self.access_df.join(
            user_res_cf_df_model.user_model_df, [tenant_col, user_col]
        ).join(
            user_res_cf_df_model.res_model_df, [tenant_col, res_col]
        ).select(
            tenant_col,
            user_col,
            user_vec_col,
            res_col,
            res_vec_col,
            dot(f.col(user_vec_col), f.col(res_vec_col)).alias(likelihood_col_token)
        )

        scaler_model = PartitionedStandardScaler(
            inputCol=likelihood_col_token,
            partitionKey=tenant_col,
            outputCol=user_vec_col
        ).fit(fixed_df)

        per_group_stats: DataFrame = scaler_model.getPerGroupStats()
        assert isinstance(per_group_stats, DataFrame)

        append2user_bias = self._make_append_bias(user_col, res_col, user_col, user_col, self.rank)
        append2res_bias = self._make_append_bias(user_col, res_col, res_col, user_col, self.rank)

        fixed_user_model_df = user_res_cf_df_model.user_model_df.join(
            per_group_stats, tenant_col
        ).select(
            tenant_col,
            user_col,
            append2user_bias(
                f.col(user_vec_col),
                f.lit(-1.0) * f.col("stats.mean"),
                f.lit(-1.0) / f.when(
                    f.col("stats.std") != 0.0,
                    f.col("stats.std")
                ).otherwise(f.lit(1.0))
            ).alias(user_vec_col)
        )

        fixed_res_model_df = user_res_cf_df_model.res_model_df.join(
            per_group_stats, tenant_col
        ).select(
            tenant_col,
            res_col,
            append2res_bias(f.col(res_vec_col), f.lit(0)).alias(res_vec_col)
        )

        return user_res_cf_df_model.replace_models(fixed_user_model_df, fixed_res_model_df)
