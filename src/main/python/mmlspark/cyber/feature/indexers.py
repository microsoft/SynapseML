__author__ = 'rolevin'

from typing import List

from mmlspark.cyber.utils.spark_utils import DataFrameUtils, ExplainBuilder

from pyspark.ml import Estimator, Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params
from pyspark.sql import DataFrame, functions as f


class IdIndexerModel(Transformer, HasInputCol, HasOutputCol):
    partitionKey = Param(
        Params._dummy(),
        "partitionKey",
        "The name of the column to partition by, i.e., make sure the indexing takes the partition into account. "
        "This is exemplified in reset_per_partition."
    )

    def __init__(self, input_col: str, partition_key: str, output_col: str, vocab_df: DataFrame):
        super().__init__()
        ExplainBuilder.build(self, inputCol=input_col, partitionKey=partition_key, outputCol=output_col)
        self._vocab_df = vocab_df

    def undo_transform(self, df: DataFrame) -> DataFrame:
        ucols = [self.partition_key, self.output_col]
        vocab_df = self._vocab_df

        return df.join(vocab_df, on=ucols, how='left_outer')

    def _transform(self, df):
        ucols = [self.partition_key, self.input_col]

        input_col = self.input_col
        output_col = self.output_col
        vocab_df = self._vocab_df

        return df.join(vocab_df, on=ucols, how='left_outer').withColumn(
            output_col,
            f.when(f.col(output_col).isNotNull(), f.col(output_col)).otherwise(f.lit(0))
        ).drop(
            input_col
        )


class IdIndexer(Estimator, HasInputCol, HasOutputCol):
    partitionKey = Param(
        Params._dummy(),
        "partitionKey",
        "The name of the column to partition by, i.e., make sure the indexing takes the partition into account. "
        "This is exemplified in reset_per_partition."
    )

    resetPerPartition = Param(
        Params._dummy(),
        "resetPerPartition",
        "When set to True then indexing is consecutive from [1..n] for each value of the partition column. "
        "When set to False then indexing is consecutive for all partition and column values."
    )

    def __init__(self, input_col: str, partition_key: str, output_col: str, reset_per_partition: bool):
        super().__init__()

        ExplainBuilder.build(
            self,
            inputCol=input_col,
            partitionKey=partition_key,
            outputCol=output_col,
            resetPerPartition=reset_per_partition
        )

    def _make_vocab_df(self, df):
        ucols = [self.getPartitionKey(), self.getInputCol()]
        the_df = df.select(ucols).distinct().orderBy(ucols)

        return DataFrameUtils.zip_with_index(
            df=the_df,
            start_index=1,
            col_name=self.getOutputCol(),
            partition_col=self.getPartitionKey(),
            order_by_col=self.getInputCol()
        ) if self.getResetPerPartition() else DataFrameUtils.zip_with_index(
            df=the_df,
            start_index=1,
            col_name=self.getOutputCol()
        )

    def _fit(self, df: DataFrame) -> IdIndexerModel:
        return IdIndexerModel(
            self.input_col, self.partition_key, self.output_col, self._make_vocab_df(df).cache()
        )


class MultiIndexerModel(Transformer):
    def __init__(self, models: List[IdIndexerModel]):
        super().__init__()
        self.models = models

    def get_model_by_input_col(self, input_col):
        for m in self.models:
            if m.input_col == input_col:
                return m

        return None

    def get_model_by_output_col(self, output_col):
        for m in self.models:
            if m.output_col == output_col:
                return m

        return None

    def undo_transform(self, df: DataFrame) -> DataFrame:
        curr_df = df.cache()

        for model in self.models:
            curr_df = model.undo_transform(curr_df).cache()

        return curr_df

    def _transform(self, df: DataFrame) -> DataFrame:
        curr_df = df.cache()

        for model in self.models:
            curr_df = model.transform(curr_df).cache()

        return curr_df


class MultiIndexer(Estimator):
    def __init__(self, indexers: List[IdIndexer]):
        super().__init__()
        self.indexers = indexers

    def _fit(self, df: DataFrame) -> MultiIndexerModel:
        return MultiIndexerModel([i.fit(df) for i in self.indexers])
