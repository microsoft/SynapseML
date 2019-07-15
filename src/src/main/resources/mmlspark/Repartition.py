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
class Repartition(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    Partitions the dataset into n partitions.  Default value for n is 10.

    Args:

        disable (bool): Whether to disable repartitioning (so that one can turn it off for evaluation) (default: false)
        n (int): Number of partitions
    """

    @keyword_only
    def __init__(self, disable=False, n=None):
        super(Repartition, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.Repartition")
        self.disable = Param(self, "disable", "disable: Whether to disable repartitioning (so that one can turn it off for evaluation) (default: false)")
        self._setDefault(disable=False)
        self.n = Param(self, "n", "n: Number of partitions")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, disable=False, n=None):
        """
        Set the (keyword only) parameters

        Args:

            disable (bool): Whether to disable repartitioning (so that one can turn it off for evaluation) (default: false)
            n (int): Number of partitions
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setDisable(self, value):
        """

        Args:

            disable (bool): Whether to disable repartitioning (so that one can turn it off for evaluation) (default: false)

        """
        self._set(disable=value)
        return self


    def getDisable(self):
        """

        Returns:

            bool: Whether to disable repartitioning (so that one can turn it off for evaluation) (default: false)
        """
        return self.getOrDefault(self.disable)


    def setN(self, value):
        """

        Args:

            n (int): Number of partitions

        """
        self._set(n=value)
        return self


    def getN(self):
        """

        Returns:

            int: Number of partitions
        """
        return self.getOrDefault(self.n)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.Repartition"

    @staticmethod
    def _from_java(java_stage):
        module_name=Repartition.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".Repartition"
        return from_java(java_stage, module_name)
