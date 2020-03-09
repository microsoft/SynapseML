from typing import List, Tuple

from pyspark import SQLContext

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params

from mmlspark.cyber.ml.utils.spark_utils import DataFrameUtils, ExplainBuilder


class TestDataFrameUtils:
    def create_sample_dataframe(self, spark_context: SQLContext):
        schema = StructType(
            [
                StructField("tenant", StringType(), nullable=True),
                StructField("user", StringType(), nullable=True)
            ]
        )
        dataframe = spark_context.createDataFrame(
            [
                ("OrgA", "Alice"),
                ("OrgB", "Joe"),
                ("OrgA", "Joe"),
                ("OrgA", "Bob")
            ],
            schema
        )
        return dataframe

    def create_string_type_dataframe(self,
                                     spark_context: SQLContext,
                                     field_names: List[str],
                                     data: List[Tuple[str]]) -> DataFrame:

        return spark_context.createDataFrame(
            data,
            StructType([StructField(name, StringType(), nullable=True) for name in field_names])
        )

    def test_get_spark(self, spark_context: SQLContext):
        df = self.create_sample_dataframe(spark_context)
        assert df is not None

        spark = DataFrameUtils.get_spark_session(df)

        assert spark is not None
        assert spark is spark_context.sparkSession

    def test_zip_with_index_sort_by_column_within_partitions(self, spark_context: SQLContext):
        dataframe = self.create_sample_dataframe(spark_context)
        result = DataFrameUtils.zip_with_index(df=dataframe, partition_col="tenant", order_by_col="user")
        expected = [
            ("OrgB", "Joe", 0),
            ("OrgA", "Alice", 0),
            ("OrgA", "Bob", 1),
            ("OrgA", "Joe", 2)
        ]
        assert result.collect() == expected

    def test_zip_without_partitions_sort_by_column(self, spark_context: SQLContext):
        dataframe = self.create_sample_dataframe(spark_context)
        result = DataFrameUtils.zip_with_index(df=dataframe, order_by_col="user")
        expected = [
            ("OrgA", "Alice", 0),
            ("OrgA", "Bob", 1),
            ("OrgB", "Joe", 2),
            ("OrgA", "Joe", 3)
        ]
        assert result.collect() == expected


class TestExplainBuilder:
    class ExplainableObj(Transformer, HasInputCol, HasOutputCol):
        partitionKey = Param(
            Params._dummy(),
            "partitionKey",
            "The name of the column to partition by."
        )

        secondPartitionKey = Param(
            Params._dummy(),
            "secondPartitionKey",
            "The name of the column to partition by."
        )

        def __init__(self):
            super().__init__()
            ExplainBuilder.build(self, inputCol='input', partitionKey=1)
            self.setSecondPartitionKey(2)
            self.setOutputCol('output')

        def _transform(self, dataset):
            pass

    def test_explain(self):
        oo = TestExplainBuilder.ExplainableObj()

        assert oo.getPartitionKey() == 1
        assert oo.partition_key == 1

        assert oo.getSecondPartitionKey() == 2
        assert oo.second_partition_key == 2

        assert oo.getInputCol() == 'input'
        assert oo.input_col == 'input'

        assert oo.getOutputCol() == 'output'
        assert oo.output_col == 'output'

    def test_explain_with_modify(self):
        oo = TestExplainBuilder.ExplainableObj()

        assert oo.partition_key == oo.getPartitionKey()
        assert oo.partition_key == 1

        oo.partition_key = 5

        assert oo.partition_key == 5
        assert oo.getPartitionKey() == 5

    def test_explain_with_modify_with_setter(self):
        oo = TestExplainBuilder.ExplainableObj()

        assert oo.partition_key == oo.getPartitionKey()
        assert oo.partition_key == 1

        oo.setPartitionKey(5)

        assert oo.partition_key == 5
        assert oo.getPartitionKey() == 5
