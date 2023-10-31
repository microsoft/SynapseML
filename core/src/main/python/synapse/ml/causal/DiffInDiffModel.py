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

        ctx = SparkContext._active_spark_context
        sql_ctx = SQLContext.getOrCreate(ctx)

        self.summary = java_obj.getSummary()
        self.treatmentEffect = self.summary.treatmentEffect()
        self.standardError = self.summary.standardError()
        self.timeIntercept = DiffInDiffModel._unwrapOption(self.summary.timeIntercept())
        self.unitIntercept = DiffInDiffModel._unwrapOption(self.summary.unitIntercept())
        self.timeWeights = DiffInDiffModel._mapOption(
            java_obj.getTimeWeights(), lambda x: DataFrame(x, sql_ctx)
        )
        self.unitWeights = DiffInDiffModel._mapOption(
            java_obj.getUnitWeights(), lambda x: DataFrame(x, sql_ctx)
        )
        self.timeRMSE = DiffInDiffModel._unwrapOption(self.summary.timeRMSE())
        self.unitRMSE = DiffInDiffModel._unwrapOption(self.summary.unitRMSE())
        self.zeta = DiffInDiffModel._unwrapOption(self.summary.zeta())
        self.lossHistoryTimeWeights = DiffInDiffModel._unwrapOption(
            self.summary.getLossHistoryTimeWeightsJava()
        )
        self.lossHistoryUnitWeights = DiffInDiffModel._unwrapOption(
            self.summary.getLossHistoryUnitWeightsJava()
        )
