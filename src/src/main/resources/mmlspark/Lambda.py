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
from mmlspark.TypeConversionUtils import generateTypeConverter, complexTypeConverter

@inherit_doc
class Lambda(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        transformFunc (object): holder for dataframe function
        transformSchemaFunc (object): the output schema after the transformation
    """

    @keyword_only
    def __init__(self, transformFunc=None, transformSchemaFunc=None):
        super(Lambda, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.Lambda")
        self._cache = {}
        self.transformFunc = Param(self, "transformFunc", "transformFunc: holder for dataframe function", generateTypeConverter("transformFunc", self._cache, complexTypeConverter))
        self.transformSchemaFunc = Param(self, "transformSchemaFunc", "transformSchemaFunc: the output schema after the transformation", generateTypeConverter("transformSchemaFunc", self._cache, complexTypeConverter))
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, transformFunc=None, transformSchemaFunc=None):
        """
        Set the (keyword only) parameters

        Args:

            transformFunc (object): holder for dataframe function
            transformSchemaFunc (object): the output schema after the transformation
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setTransformFunc(self, value):
        """

        Args:

            transformFunc (object): holder for dataframe function

        """
        self._set(transformFunc=value)
        return self


    def getTransformFunc(self):
        """

        Returns:

            object: holder for dataframe function
        """
        return self._cache.get("transformFunc", None)


    def setTransformSchemaFunc(self, value):
        """

        Args:

            transformSchemaFunc (object): the output schema after the transformation

        """
        self._set(transformSchemaFunc=value)
        return self


    def getTransformSchemaFunc(self):
        """

        Returns:

            object: the output schema after the transformation
        """
        return self._cache.get("transformSchemaFunc", None)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.Lambda"

    @staticmethod
    def _from_java(java_stage):
        module_name=Lambda.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".Lambda"
        return from_java(java_stage, module_name)
