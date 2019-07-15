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
class Cacher(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        disable (bool): Whether or disable caching (so that you can turn it off during evaluation) (default: false)
    """

    @keyword_only
    def __init__(self, disable=False):
        super(Cacher, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.Cacher")
        self.disable = Param(self, "disable", "disable: Whether or disable caching (so that you can turn it off during evaluation) (default: false)")
        self._setDefault(disable=False)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, disable=False):
        """
        Set the (keyword only) parameters

        Args:

            disable (bool): Whether or disable caching (so that you can turn it off during evaluation) (default: false)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setDisable(self, value):
        """

        Args:

            disable (bool): Whether or disable caching (so that you can turn it off during evaluation) (default: false)

        """
        self._set(disable=value)
        return self


    def getDisable(self):
        """

        Returns:

            bool: Whether or disable caching (so that you can turn it off during evaluation) (default: false)
        """
        return self.getOrDefault(self.disable)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.Cacher"

    @staticmethod
    def _from_java(java_stage):
        module_name=Cacher.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".Cacher"
        return from_java(java_stage, module_name)
