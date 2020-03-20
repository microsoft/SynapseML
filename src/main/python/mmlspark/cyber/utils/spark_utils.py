from typing import List, Union

from pyspark.ml import Estimator, Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql import DataFrame, SparkSession, functions as f, types as t
from pyspark.sql.window import Window

__all__ = ["ExplainBuilder"]


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
        return this._set(**{param_name: value})

    return set_param


class ExplainBuilder:
    @staticmethod
    def build(explainable: Union[Estimator, Transformer], **kwargs):
        # copy to avoid changing while iterating
        param_map = dict(explainable.__dict__)

        for varname, vv in param_map.items():
            if not isinstance(varname, str) or not isinstance(vv, Param):
                continue

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

            setattr(explainable.__class__, f"{param_name}", property(getter, setter))

        explainable._set(**kwargs)
