#  Copyright (C) Microsoft Corporation. All rights reserved.
#  Licensed under the MIT License. See LICENSE in project root for information.


import sys

if sys.version >= "3":
    basestring = str

from pyspark.sql import DataFrame
from pyspark.ml.param.shared import *
from pyspark import keyword_only
from mmlspark.core.serialize.java_params_patch import *
from pyspark.ml.common import inherit_doc
from mmlspark.core.schema.Utils import *
from pyspark.ml.param import TypeConverters
from mmlspark.explainers._LocalExplainer import _LocalExplainer


@inherit_doc
class VectorSHAP(_LocalExplainer):
    """
    Args:
        inputCol (object): input column name
        metricsCol (object): Column name for fitting metrics
        model (object): The model to be interpreted.
        numSamples (int): Number of samples to generate.
        outputCol (object): output column name
        targetClass (int): The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        targetClassCol (object): The name of the column that specifies the index of the class for multinomial classification models.
        targetCol (object): The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
    """

    inputCol = Param(Params._dummy(), "inputCol", "input column name")

    metricsCol = Param(Params._dummy(), "metricsCol", "Column name for fitting metrics")

    model = Param(Params._dummy(), "model", "The model to be interpreted.")

    numSamples = Param(
        Params._dummy(), "numSamples", "Number of samples to generate.", typeConverter=TypeConverters.toInt
    )

    outputCol = Param(Params._dummy(), "outputCol", "output column name")

    targetClass = Param(
        Params._dummy(),
        "targetClass",
        "The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.",
        typeConverter=TypeConverters.toInt,
    )

    targetClassCol = Param(
        Params._dummy(),
        "targetClassCol",
        "The name of the column that specifies the index of the class for multinomial classification models.",
    )

    targetCol = Param(
        Params._dummy(),
        "targetCol",
        'The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability',
    )

    @keyword_only
    def __init__(
        self,
        java_obj=None,
        inputCol=None,
        metricsCol="r2",
        model=None,
        numSamples=None,
        outputCol="VectorSHAP_304269cdaa9f__output",
        targetClass=0,
        targetClassCol=None,
        targetCol="probability",
        backgroundData: DataFrame = None,
    ):
        super(VectorSHAP, self).__init__()
        if java_obj is None:
            self._java_obj = self._new_java_obj("com.microsoft.ml.spark.explainers.VectorSHAP", self.uid)
        else:
            self._java_obj = java_obj
        self._setDefault(metricsCol="r2")
        self._setDefault(outputCol="VectorSHAP_304269cdaa9f__output")
        self._setDefault(targetClass=0)
        self._setDefault(targetCol="probability")
        self.setBackgroundData(backgroundData)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs

        if java_obj is None:
            for k, v in kwargs.items():
                if v is not None:
                    getattr(self, "set" + k[0].upper() + k[1:])(v)

    @keyword_only
    def setParams(
        self,
        inputCol=None,
        metricsCol="r2",
        model=None,
        numSamples=None,
        outputCol="VectorSHAP_304269cdaa9f__output",
        targetClass=0,
        targetClassCol=None,
        targetCol="probability",
    ):
        """
        Set the (keyword only) parameters
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.explainers.VectorSHAP"

    @staticmethod
    def _from_java(java_stage):
        module_name = VectorSHAP.__module__
        module_name = module_name.rsplit(".", 1)[0] + ".VectorSHAP"
        return from_java(java_stage, module_name)

    def setBackgroundData(self, value: DataFrame):
        """
        Args:
            value: A Spark DataFrame holding the background dataset.
        """
        if isinstance(value, DataFrame):
            self._java_obj.setBackgroundDataset(value._jdf)
        else:
            raise ValueError("The value is not a Spark DataFrame.")

    def setInputCol(self, value):
        """
        Args:
            inputCol: input column name
        """

        self._set(inputCol=value)
        return self

    def setMetricsCol(self, value):
        """
        Args:
            metricsCol: Column name for fitting metrics
        """

        self._set(metricsCol=value)
        return self

    def setModel(self, value):
        """
        Args:
            model: The model to be interpreted.
        """

        self._set(model=value)
        return self

    def setNumSamples(self, value):
        """
        Args:
            numSamples: Number of samples to generate.
        """

        self._set(numSamples=value)
        return self

    def setOutputCol(self, value):
        """
        Args:
            outputCol: output column name
        """

        self._set(outputCol=value)
        return self

    def setTargetClass(self, value):
        """
        Args:
            targetClass: The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        """

        self._set(targetClass=value)
        return self

    def setTargetClassCol(self, value):
        """
        Args:
            targetClassCol: The name of the column that specifies the index of the class for multinomial classification models.
        """

        self._set(targetClassCol=value)
        return self

    def setTargetCol(self, value):
        """
        Args:
            targetCol: The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
        """

        self._set(targetCol=value)
        return self

    def getInputCol(self):
        """
        Returns:
            inputCol: input column name
        """

        return self.getOrDefault(self.inputCol)

    def getMetricsCol(self):
        """
        Returns:
            metricsCol: Column name for fitting metrics
        """

        return self.getOrDefault(self.metricsCol)

    def getModel(self):
        """
        Returns:
            model: The model to be interpreted.
        """

        return JavaParams._from_java(self._java_obj.getModel())

    def getNumSamples(self):
        """
        Returns:
            numSamples: Number of samples to generate.
        """

        return self.getOrDefault(self.numSamples)

    def getOutputCol(self):
        """
        Returns:
            outputCol: output column name
        """

        return self.getOrDefault(self.outputCol)

    def getTargetClass(self):
        """
        Returns:
            targetClass: The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        """

        return self.getOrDefault(self.targetClass)

    def getTargetClassCol(self):
        """
        Returns:
            targetClassCol: The name of the column that specifies the index of the class for multinomial classification models.
        """

        return self.getOrDefault(self.targetClassCol)

    def getTargetCol(self):
        """
        Returns:
            targetCol: The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
        """

        return self.getOrDefault(self.targetCol)
