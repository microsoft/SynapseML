__author__ = 'rolevin'

from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Union

from mmlspark.cyber.utils.spark_utils import ExplainBuilder

from pyspark.ml import Estimator, Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params
from pyspark.sql import DataFrame, functions as f, types as t


def _pyudf(func, use_pandas):
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf
    return pandas_udf(func, t.DoubleType(), PandasUDFType.SCALAR) if use_pandas else udf(func, t.DoubleType())


class PerPartitionScalarScalerModel(ABC, Transformer, HasInputCol, HasOutputCol):
    partitionKey = Param(
        Params._dummy(),
        "partitionKey",
        "The name of the column to partition by, i.e., scale the values of inputCol within each partition. "
    )

    def __init__(self,
                 input_col: str,
                 partition_key: Optional[str],
                 output_col: str,
                 per_group_stats: Union[DataFrame, Dict[str, float]],
                 use_pandas: bool = True):

        super().__init__()
        ExplainBuilder.build(self, inputCol=input_col, partitionKey=partition_key, outputCol=output_col)
        self._per_group_stats = per_group_stats
        self._use_pandas = use_pandas

    @property
    def per_group_stats(self):
        return self._per_group_stats

    @property
    def use_pandas(self):
        return self._use_pandas

    @abstractmethod
    def _make_partitioned_stats_method(self) -> Callable:
        raise NotImplementedError

    @abstractmethod
    def _make_unpartitioned_stats_method(self) -> Callable:
        raise NotImplementedError

    def is_partitioned(self) -> bool:
        if self.partition_key is not None or isinstance(self.per_group_stats, DataFrame):
            assert self.partition_key is not None and isinstance(self.per_group_stats, DataFrame)
            res = True
        elif self.partition_key is None or isinstance(self.per_group_stats, Dict):
            assert self.partition_key is None and isinstance(self.per_group_stats, Dict)
            res = False
        else:
            assert False, 'unsupported type for per_group_stats: {0}'.format(type(self.per_group_stats))

        return res

    def _make_stats_method(self) -> Callable:
        return self._make_partitioned_stats_method() if self.is_partitioned() else _pyudf(
            self._make_unpartitioned_stats_method(), self.use_pandas
        )

    def _transform(self, df: DataFrame) -> DataFrame:
        stats_method = self._make_stats_method()
        input_col = self.input_col
        output_col = self.output_col

        if self.is_partitioned():
            partition_key = self.partition_key
            per_group_stats = self.per_group_stats

            with_stats_df = df.join(per_group_stats, partition_key, how='left')
        else:
            with_stats_df = df

        return with_stats_df.withColumn(output_col, stats_method(f.col(input_col)))


class PerPartitionScalarScalerEstimator(ABC, Estimator, HasInputCol, HasOutputCol):
    partitionKey = Param(
        Params._dummy(),
        "partitionKey",
        "The name of the column to partition by, i.e., scale the values of inputCol within each partition. "
    )

    def __init__(self,
                 input_col: str,
                 partition_key: Optional[str],
                 output_col: str,
                 use_pandas: bool = True):

        super().__init__()
        ExplainBuilder.build(self, inputCol=input_col, partitionKey=partition_key, outputCol=output_col)
        self._use_pandas = use_pandas

    @property
    def use_pandas(self):
        return self._use_pandas

    @abstractmethod
    def _apply_on_cols(self) -> List[Callable]:
        raise NotImplementedError()

    @abstractmethod
    def _create_model(self, per_group_stats: Union[DataFrame, Dict[str, float]]) -> PerPartitionScalarScalerModel:
        raise NotImplementedError()

    def _fit(self, df: DataFrame) -> PerPartitionScalarScalerModel:
        partition_key = self.partition_key
        apply_on_cols = self._apply_on_cols()

        if partition_key is None:
            rows = df.select(*apply_on_cols).collect()
            assert len(rows) == 1
            per_group_stats = rows[0].asDict()
        else:
            per_group_stats = df.groupBy(partition_key).agg(*apply_on_cols)

        assert per_group_stats is not None
        return self._create_model(per_group_stats)


class StandardScalarScalerConfig:
    """
    The tokens to use for temporary representation of mean and standard deviation
    """
    mean_token = '__mean__'
    std_token = '__std__'


class StandardScalarScalerModel(PerPartitionScalarScalerModel):
    coefficientFactor = Param(
        Params._dummy(),
        "coefficientFactor",
        "After scaling values of outputCol are multiplied by coefficient (defaults to 1.0). "
    )

    def __init__(self,
                 input_col: str,
                 partition_key: Optional[str],
                 output_col: str,
                 per_group_stats: Union[DataFrame, Dict[str, float]],
                 coefficient_factor: float = 1.0,
                 use_pandas: bool = True):

        super().__init__(input_col, partition_key, output_col, per_group_stats, use_pandas)
        self.coefficient_factor = coefficient_factor

    def _make_partitioned_stats_method(self) -> Callable:
        assert isinstance(self.per_group_stats, DataFrame)
        coefficient_factor = self.coefficient_factor

        def norm(x):
            return f.when(f.col(StandardScalarScalerConfig.std_token) != 0.0,
                          f.lit(coefficient_factor) * ((x - f.col(
                              StandardScalarScalerConfig.mean_token
                          )) / f.col(
                              StandardScalarScalerConfig.std_token
                          ))).otherwise(x - f.col(StandardScalarScalerConfig.mean_token))

        return norm

    def _make_unpartitioned_stats_method(self) -> Callable:
        assert isinstance(self.per_group_stats, dict)

        mean = self.per_group_stats[StandardScalarScalerConfig.mean_token]
        std = self.per_group_stats[StandardScalarScalerConfig.std_token]
        coefficient_factor = self.coefficient_factor

        assert mean is not None
        assert std is not None

        def norm(x):
            return coefficient_factor * (x - mean) / std

        return norm


class StandardScalarScaler(PerPartitionScalarScalerEstimator):
    coefficientFactor = Param(
        Params._dummy(),
        "coefficientFactor",
        "After scaling values of outputCol are multiplied by coefficient (defaults to 1.0). "
    )

    def __init__(self,
                 input_col: str,
                 partition_key: Optional[str],
                 output_col: str,
                 coefficient_factor: float = 1.0,
                 use_pandas: bool = True):

        super().__init__(input_col, partition_key, output_col, use_pandas)
        self.coefficient_factor = coefficient_factor

    def _apply_on_cols(self) -> List[Callable]:
        input_col = self.input_col

        return [
            f.mean(f.col(input_col)).alias(StandardScalarScalerConfig.mean_token),
            f.stddev_pop(f.col(input_col)).alias(StandardScalarScalerConfig.std_token)
        ]

    def _create_model(self, per_group_stats: Union[DataFrame, Dict[str, float]]) -> PerPartitionScalarScalerModel:
        return StandardScalarScalerModel(
            self.input_col,
            self.partition_key,
            self.output_col,
            per_group_stats,
            self.coefficient_factor,
            self.use_pandas
        )


class LinearScalarScalerConfig:
    min_actual_value_token = '__min_actual_value__'
    max_actual_value_token = '__max_actual_value__'


class LinearScalarScalerModel(PerPartitionScalarScalerModel):
    def __init__(self,
                 input_col: str,
                 partition_key: Optional[str],
                 output_col: str,
                 per_group_stats: Union[DataFrame, Dict[str, float]],
                 min_required_value: float,
                 max_required_value: float,
                 use_pandas: bool = True):

        super().__init__(input_col, partition_key, output_col, per_group_stats, use_pandas)
        self.min_required_value = min_required_value
        self.max_required_value = max_required_value

    def _make_partitioned_stats_method(self) -> Callable:
        req_delta = self.max_required_value - self.min_required_value

        def actual_delta():
            return f.col(
                LinearScalarScalerConfig.max_actual_value_token
            ) - f.col(
                LinearScalarScalerConfig.min_actual_value_token
            )

        def a():
            return f.when(actual_delta() != f.lit(0), f.lit(req_delta) / actual_delta()).otherwise(f.lit(0.0))

        def b():
            return f.when(actual_delta() != f.lit(0),
                          self.max_required_value - a() * f.col(
                              LinearScalarScalerConfig.max_actual_value_token)
                          ).otherwise(f.lit((self.min_required_value + self.max_required_value) / 2.0))

        def norm(x):
            return a() * x + b()

        return norm

    def _make_unpartitioned_stats_method(self) -> Callable:
        assert isinstance(self.per_group_stats, Dict)

        min_actual_value = self.per_group_stats[LinearScalarScalerConfig.min_actual_value_token]
        max_actual_value = self.per_group_stats[LinearScalarScalerConfig.max_actual_value_token]

        delta = max_actual_value - min_actual_value

        a = (self.max_required_value - self.min_required_value) / delta if delta != 0.0 else 0.0
        b = self.max_required_value - a * max_actual_value if delta != 0.0 else \
            (self.min_required_value + self.max_required_value) / 2.0

        def norm(x):
            return a * x + b

        return norm


class LinearScalarScaler(PerPartitionScalarScalerEstimator):
    minRequiredValue = Param(
        Params._dummy(),
        "minRequiredValue",
        "Scale the outputCol to have a value between [minRequiredValue, maxRequiredValue]."
    )

    maxRequiredValue = Param(
        Params._dummy(),
        "maxRequiredValue",
        "Scale the outputCol to have a value between [minRequiredValue, maxRequiredValue]."
    )

    def __init__(self,
                 input_col: str,
                 partition_key: Optional[str],
                 output_col: str,
                 min_required_value: float = 0.0,
                 max_required_value: float = 1.0,
                 use_pandas: bool = True):

        super().__init__(input_col, partition_key, output_col, use_pandas)
        self.min_required_value = min_required_value
        self.max_required_value = max_required_value

    def _apply_on_cols(self) -> List[Callable]:
        input_col = self.input_col

        return [
            f.min(f.col(input_col)).alias(LinearScalarScalerConfig.min_actual_value_token),
            f.max(f.col(input_col)).alias(LinearScalarScalerConfig.max_actual_value_token)
        ]

    def _create_model(self, per_group_stats: Union[DataFrame, Dict[str, float]]) -> PerPartitionScalarScalerModel:
        return LinearScalarScalerModel(
            self.input_col,
            self.partition_key,
            self.output_col,
            per_group_stats,
            self.min_required_value,
            self.max_required_value,
            self.use_pandas
        )
