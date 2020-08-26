__author__ = 'rolevin'

from typing import List, Optional

from mmlspark.cyber.utils.spark_utils import DataFrameUtils, ExplainBuilder

from pyspark.ml import Transformer
from pyspark.ml.param.shared import Param, Params
from pyspark.sql import DataFrame, functions as f, types as t
import random


class ComplementAccessTransformer(Transformer):
    partitionKey = Param(
        Params._dummy(),
        "partitionKey",
        "The name of the partition_key field name"
    )

    indexedColNamesArr = Param(
        Params._dummy(),
        "indexedColNamesArr",
        "The name of the fields to use to generate the complement set from"
    )

    complementsetFactor = Param(
        Params._dummy(),
        "complementsetFactor",
        "The estimated average size of the complement set to generate"
    )

    """
    Given a dataframe it returns a new dataframe with access patterns sampled from
    the set of possible access patterns which did not occur in the given dataframe
    (i.e., it returns a sample from the complement set).
    """

    def __init__(self,
                 partition_key: Optional[str],
                 indexed_col_names_arr: List[str],
                 complementset_factor: int):

        super().__init__()

        # we assume here that all indices of the columns are continuous within their partition_key
        ExplainBuilder.build(
            self,
            partitionKey=partition_key,
            indexedColNamesArr=indexed_col_names_arr,
            complementsetFactor=complementset_factor
        )

    @staticmethod
    def _min_index_token(curr_col_name: str) -> str:
        return '__min_{0}__'.format(curr_col_name)

    @staticmethod
    def _max_index_token(curr_col_name: str) -> str:
        return '__max_{0}__'.format(curr_col_name)

    @staticmethod
    def _tuple_token() -> str:
        return '__tuple__'

    def _transform(self, df: DataFrame) -> DataFrame:
        """ generate a dataframe which consists of a sample from the complement set

        Parameters
        ----------
        df: a given dataframe containing the columns in 'indexed_col_names_arr'

        Returns
        -------
        dataframe which which consists of a sample from the complement set
        """
        complementset_factor = self.complementset_factor

        if complementset_factor == 0:
            return DataFrameUtils.make_empty(df)

        the_partition_key = self.partition_key
        indexed_col_names_arr = self.indexed_col_names_arr

        if the_partition_key is None:
            partition_key = '__dummy_partition_key__'
            df = df.withColumn(partition_key, f.lit(0)).cache()
        else:
            partition_key = the_partition_key
            df = df.cache()

        limits_dfs = [df.select(
            partition_key, curr_col_name
        ).distinct().groupBy(partition_key).agg(
            f.min(curr_col_name).alias(ComplementAccessTransformer._min_index_token(curr_col_name)),
            f.max(curr_col_name).alias(ComplementAccessTransformer._max_index_token(curr_col_name))
        ).orderBy(
            partition_key
        ) for curr_col_name in indexed_col_names_arr]

        def make_randint(factor):
            schema = t.ArrayType(
                t.StructType([t.StructField(
                    curr_col_name, t.IntegerType()
                ) for curr_col_name in indexed_col_names_arr])
            )

            @f.udf(schema)
            def randint(min_index_arr, max_index_arr):
                return [tuple([random.randint(min_index, max_index) for min_index, max_index
                               in zip(min_index_arr, max_index_arr)]) for _ in range(factor)]

            return randint

        pre_complement_candidates_df = df.cache()

        for limits_df in limits_dfs:
            pre_complement_candidates_df = pre_complement_candidates_df.join(limits_df, partition_key).cache()

        cols = [f.col(partition_key)] + [f.col(curr_col_name) for curr_col_name in indexed_col_names_arr]
        randint = make_randint(complementset_factor)

        complement_candidates_df = pre_complement_candidates_df.withColumn(
            ComplementAccessTransformer._tuple_token(),
            f.explode(randint(
                f.array([f.col(ComplementAccessTransformer._min_index_token(curr_col_name)) for curr_col_name
                         in indexed_col_names_arr]),
                f.array([f.col(ComplementAccessTransformer._max_index_token(curr_col_name)) for curr_col_name
                         in indexed_col_names_arr])
            ))
        ).select(
            *([partition_key] + [f.col('{0}.{1}'.format(
                ComplementAccessTransformer._tuple_token(),
                curr_col_name
            )).alias(curr_col_name) for curr_col_name in indexed_col_names_arr])
        ).distinct().orderBy(*cols)

        tuples_df = df.select(*cols).distinct().orderBy(*cols)

        res_df = complement_candidates_df.join(
            tuples_df,
            [partition_key] + indexed_col_names_arr,
            how='left_anti'
        ).select(*cols).orderBy(*cols)

        if the_partition_key is None:
            res_df = res_df.drop(partition_key)

        return res_df
