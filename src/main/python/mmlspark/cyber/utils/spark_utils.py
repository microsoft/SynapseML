from typing import Any, List, Union

from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql import DataFrame, SparkSession, functions as f, types as t
from pyspark.sql.window import Window

__all__ = ["DataFrameUtils", "ExplainBuilder"]


class DataFrameUtils:
    """ Extension methods over Spark DataFrame """

    @staticmethod
    def get_spark_session(df: DataFrame) -> SparkSession:
        """ get the associated Spark session

        Parameters
        ----------
        df : DataFrame
            the dataframe of which we want to get its Spark session
        """

        return df.sql_ctx.sparkSession

    @staticmethod
    def make_empty(df: DataFrame) -> DataFrame:
        """ make an empty dataframe with the same schema

        Parameters
        ----------
        df the dataframe whose schema we wish to use

        Returns an empty dataframe
        -------

        """
        return DataFrameUtils.get_spark_session(df).createDataFrame([], df.schema)

    # noinspection PyDefaultArgument
    @staticmethod
    def zip_with_index(
            df: DataFrame,
            start_index: int = 0,
            col_name: str = 'rowId',
            partition_col: Union[List[str], str] = [],
            order_by_col: Union[List[str], str] = []) -> DataFrame:
        """ add an index to the given dataframe

        Parameters
        ----------
        df : dataframe
            the dataframe to add the index to
        start_index : int
            the value to start the count from
        col_name : str
            the name of the index column which will be added as last column in the output data frame
        partition_col : Union[List[str], str]
            optional column name or list of columns names that define a partitioning to assign indices independently to,
            e.g., assign sequential indices separately to each distinct tenant
        order_by_col : Union[List[str], str]
            optional order by column name or list of columns that are used for sorting
            the data frame or partitions before indexing
        """
        if df is None:
            raise ValueError("df cannot be None")
        if col_name is None:
            raise ValueError("col_name cannot be None")
        if partition_col is None:
            raise ValueError("partition_col cannot be None")
        if order_by_col is None:
            raise ValueError("order_by_col cannot be None")

        # coalese input columns to arrays from singular strings
        partition_col = partition_col if isinstance(partition_col, list) else [partition_col]
        order_by_col = order_by_col if isinstance(order_by_col, list) else [order_by_col]

        if len(partition_col) > 0:
            partition_columns = [f.col(cn) for cn in partition_col]
            window = Window.partitionBy(*partition_columns)

            if len(order_by_col) > 0:
                order_by_columns = [f.col(cn) for cn in order_by_col]
                window = window.orderBy(*order_by_columns)

            return df.withColumn(col_name, f.row_number().over(window) - 1 + start_index)
        else:
            if len(order_by_col) > 0:
                order_by_columns = [f.col(cn) for cn in order_by_col]
                df = df.orderBy(*order_by_columns)

            output_schema = t.StructType(df.schema.fields + [t.StructField(col_name, t.LongType(), True)])
            return df.rdd.zipWithIndex().map(lambda line: (list(line[0]) + [line[1] + start_index])).toDF(output_schema)


def to_camel_case(prefix: str, varname: str) -> str:
    parts = varname.split('_')
    first = parts[0][0:1].upper() + parts[0][1:] if prefix != '' else parts[0]
    residual = [pp[0:1].upper() + pp[1:] for pp in parts[1:]]
    return prefix + ''.join([first] + residual)


def from_camel_case(cc: str) -> str:
    return ''.join([str(c) if not c.isupper() else '_' + c.lower() for c in cc])


def make_get_param(param_name: str):
    def get_param(this):
        return this.getOrDefault(getattr(this, param_name))

    return get_param


def make_set_param(param_name: str):
    def set_param(this, value):
        # noinspection PyProtectedMember
        return this._set(**{param_name: value})

    return set_param


class ExplainBuilder:
    @staticmethod
    def get_methods(the_explainable):
        return [method_name for method_name in dir(the_explainable) if callable(getattr(the_explainable, method_name))]

    @staticmethod
    def get_method(the_explainable, the_method_name):
        try:
            return getattr(type(the_explainable), the_method_name)
        except AttributeError:
            return None

    @staticmethod
    def copy_params(from_explainable: Any, to_explainable: Any):
        param_map = {
            varname: vv for varname, vv in from_explainable.__dict__.items()
            if isinstance(varname, str) and isinstance(vv, Param)
        }

        for varname, vv in param_map.items():
            setattr(to_explainable.__class__, varname, vv)

    @staticmethod
    def build(explainable: Any, **kwargs):
        # copy to avoid changing while iterating
        param_map = {
            varname: vv for varname, vv in explainable.__dict__.items()
            if isinstance(varname, str) and isinstance(vv, Param)
        }

        for varname, vv in param_map.items():
            param_name = from_camel_case(vv.name)
            assert param_name != vv.name, f"must use a camelCase name so that it is different from camel_case"

            add_getter_and_setter = True

            if isinstance(explainable, HasInputCol) and vv.name == 'inputCol':
                add_getter_and_setter = False
            elif isinstance(explainable, HasOutputCol) and vv.name == 'outputCol':
                add_getter_and_setter = False

            assert param_name == from_camel_case(varname), (param_name, varname, from_camel_case(varname))

            getter = make_get_param(varname)
            setter = make_set_param(varname)

            if add_getter_and_setter:
                setattr(explainable.__class__, to_camel_case('get', param_name), getter)
                setattr(explainable.__class__, to_camel_case('set', param_name), setter)
            else:
                assert ExplainBuilder.get_method(explainable, to_camel_case('get', param_name)) is not None, 'no_getter'
                assert ExplainBuilder.get_method(explainable, to_camel_case('set', param_name)) is not None, 'no_setter'

            setattr(explainable.__class__, f"{param_name}", property(getter, setter))

        # noinspection PyProtectedMember
        explainable._set(**kwargs)
