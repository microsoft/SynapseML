# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *

@inherit_doc
class ClassBalancer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        broadcastJoin (bool): Whether to broadcast the class to weight mapping to the worker (default: true)
        inputCol (str): The name of the input column
        outputCol (str): The name of the output column (default: weight)
    """

    @keyword_only
    def __init__(self, broadcastJoin=True, inputCol=None, outputCol="weight"):
        super(ClassBalancer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ClassBalancer")
        self.broadcastJoin = Param(self, "broadcastJoin", "broadcastJoin: Whether to broadcast the class to weight mapping to the worker (default: true)")
        self._setDefault(broadcastJoin=True)
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: weight)")
        self._setDefault(outputCol="weight")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, broadcastJoin=True, inputCol=None, outputCol="weight"):
        """
        Set the (keyword only) parameters

        Args:

            broadcastJoin (bool): Whether to broadcast the class to weight mapping to the worker (default: true)
            inputCol (str): The name of the input column
            outputCol (str): The name of the output column (default: weight)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBroadcastJoin(self, value):
        """

        Args:

            broadcastJoin (bool): Whether to broadcast the class to weight mapping to the worker (default: true)

        """
        self._set(broadcastJoin=value)
        return self


    def getBroadcastJoin(self):
        """

        Returns:

            bool: Whether to broadcast the class to weight mapping to the worker (default: true)
        """
        return self.getOrDefault(self.broadcastJoin)


    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column
        """
        return self.getOrDefault(self.inputCol)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column (default: weight)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column (default: weight)
        """
        return self.getOrDefault(self.outputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.ClassBalancer"

    @staticmethod
    def _from_java(java_stage):
        module_name=ClassBalancer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ClassBalancer"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return ClassBalancerModel(java_model)


class ClassBalancerModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`ClassBalancer`.

    This class is left empty on purpose.
    All necessary methods are exposed through inheritance.
    """

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.ClassBalancerModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=ClassBalancerModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ClassBalancerModel"
        return from_java(java_stage, module_name)

