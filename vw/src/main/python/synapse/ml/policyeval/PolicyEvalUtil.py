# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark import SparkContext


def register_policyeval_udafs():
    ctx = SparkContext._active_spark_context
    ctx._jvm.com.microsoft.azure.synapse.ml.policyeval.PolicyEvalUDAFUtil.registerUdafs()
