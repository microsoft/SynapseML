# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys


if sys.version >= "3":
    basestring = str

from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc
from mmlspark.core.schema.Utils import ComplexParamsMixin
from pyspark.sql import DataFrame
from pyspark.ml.wrapper import JavaParams, JavaWrapper


@inherit_doc
class LocalExplainer(ComplexParamsMixin, JavaParams):
    def __init__(self, java_obj=None) -> None:
        super().__init__()
        self._java_obj = java_obj

    def explain(self, dataframe: DataFrame) -> DataFrame:
        self._transfer_params_to_java()
        return DataFrame(self._java_obj.explain(dataframe._jdf), dataframe.sql_ctx)

    def _new_java_obj(self, java_class, *args):
        return JavaWrapper._new_java_obj(java_class, *args)
