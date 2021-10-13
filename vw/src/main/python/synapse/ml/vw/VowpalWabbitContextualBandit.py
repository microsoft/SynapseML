# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.vw._VowpalWabbitContextualBandit import _VowpalWabbitContextualBandit
from pyspark.ml.common import inherit_doc
from pyspark import SparkContext
from pyspark.ml.wrapper import JavaWrapper
from pyspark.ml.common import _py2java


def to_java_params(sc, model, pyParamMap):
    paramMap = JavaWrapper._new_java_obj("org.apache.spark.ml.param.ParamMap")
    for param, value in pyParamMap.items():
        java_param = model._java_obj.getParam(param.name)
        java_value = _py2java(sc, value)
        paramMap.put([java_param.w(java_value)])
    return paramMap


@inherit_doc
class VowpalWabbitContextualBandit(_VowpalWabbitContextualBandit):

    def setInitialModel(self, model):
        """
        Initialize the estimator with a previously trained model.
        """
        self._java_obj.setInitialModel(model._java_obj.getModel())

    def parallelFit(self, dataset, param_maps):
        sc = SparkContext._active_spark_context
        self._transfer_params_to_java()
        javaParamMaps = [to_java_params(sc, self, x) for x in param_maps]
        javaModels = self._java_obj.parallelFit(dataset._jdf, javaParamMaps)
        return [self._copyValues(self._create_model(x)) for x in javaModels]

