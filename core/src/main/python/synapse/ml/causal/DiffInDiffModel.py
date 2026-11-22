# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from synapse.ml.causal._DiffInDiffModel import _DiffInDiffModel
from pyspark.ml.common import inherit_doc
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SQLContext


@inherit_doc
class DiffInDiffModel(_DiffInDiffModel):
    @staticmethod
    def _mapOption(option, func):
        return func(option.get()) if option.isDefined() else None

    @staticmethod
    def _unwrapOption(option):
        return DiffInDiffModel._mapOption(option, lambda x: x)

    def __init__(self, java_obj=None) -> None:
        super(DiffInDiffModel, self).__init__(java_obj=java_obj)

    @property
    def summary(self):
        return self._java_obj.getSummary()

    @property
    def treatmentEffect(self):
        return self.summary.treatmentEffect()

    @property
    def standardError(self):
        return self.summary.standardError()

    @property
    def timeIntercept(self):
        return DiffInDiffModel._unwrapOption(self.summary.timeIntercept())

    @property
    def unitIntercept(self):
        return DiffInDiffModel._unwrapOption(self.summary.unitIntercept())

    @property
    def timeWeights(self):
        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)
        return DiffInDiffModel._mapOption(
            self._java_obj.getTimeWeights(), lambda x: DataFrame(x, sql_ctx)
        )

    @property
    def unitWeights(self):
        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)
        return DiffInDiffModel._mapOption(
            self._java_obj.getUnitWeights(), lambda x: DataFrame(x, sql_ctx)
        )

    @property
    def timeRMSE(self):
        return DiffInDiffModel._unwrapOption(self.summary.timeRMSE())

    @property
    def unitRMSE(self):
        return DiffInDiffModel._unwrapOption(self.summary.unitRMSE())

    @property
    def zeta(self):
        return DiffInDiffModel._unwrapOption(self.summary.zeta())

    @property
    def lossHistoryTimeWeights(self):
        return DiffInDiffModel._unwrapOption(
            self.summary.getLossHistoryTimeWeightsJava()
        )

    @property
    def lossHistoryUnitWeights(self):
        return DiffInDiffModel._unwrapOption(
            self.summary.getLossHistoryUnitWeightsJava()
        )
