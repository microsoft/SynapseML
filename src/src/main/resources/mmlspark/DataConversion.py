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
class DataConversion(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    Converts the specified list of columns to the specified type.  The types
    are specified by the following strings:

    - "boolean"
    - "byte"
    - "short"
    - "integer"
    - "long"
    - "float"
    - "double"
    - "string"
    - "toCategorical" - make the column be a categorical column
    - "clearCategorical" - clear the categorical column
    - "date" - the default date format is: "yyyy-MM-dd HH:mm:ss"

    Args:

        cols (list): Comma separated list of columns whose type will be converted
        convertTo (str): The result type (default: )
        dateTimeFormat (str): Format for DateTime when making DateTime:String conversions (default: yyyy-MM-dd HH:mm:ss)
    """

    @keyword_only
    def __init__(self, cols=None, convertTo="", dateTimeFormat="yyyy-MM-dd HH:mm:ss"):
        super(DataConversion, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.DataConversion")
        self.cols = Param(self, "cols", "cols: Comma separated list of columns whose type will be converted")
        self.convertTo = Param(self, "convertTo", "convertTo: The result type (default: )")
        self._setDefault(convertTo="")
        self.dateTimeFormat = Param(self, "dateTimeFormat", "dateTimeFormat: Format for DateTime when making DateTime:String conversions (default: yyyy-MM-dd HH:mm:ss)")
        self._setDefault(dateTimeFormat="yyyy-MM-dd HH:mm:ss")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, cols=None, convertTo="", dateTimeFormat="yyyy-MM-dd HH:mm:ss"):
        """
        Set the (keyword only) parameters

        Args:

            cols (list): Comma separated list of columns whose type will be converted
            convertTo (str): The result type (default: )
            dateTimeFormat (str): Format for DateTime when making DateTime:String conversions (default: yyyy-MM-dd HH:mm:ss)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setCols(self, value):
        """

        Args:

            cols (list): Comma separated list of columns whose type will be converted

        """
        self._set(cols=value)
        return self


    def getCols(self):
        """

        Returns:

            list: Comma separated list of columns whose type will be converted
        """
        return self.getOrDefault(self.cols)


    def setConvertTo(self, value):
        """

        Args:

            convertTo (str): The result type (default: )

        """
        self._set(convertTo=value)
        return self


    def getConvertTo(self):
        """

        Returns:

            str: The result type (default: )
        """
        return self.getOrDefault(self.convertTo)


    def setDateTimeFormat(self, value):
        """

        Args:

            dateTimeFormat (str): Format for DateTime when making DateTime:String conversions (default: yyyy-MM-dd HH:mm:ss)

        """
        self._set(dateTimeFormat=value)
        return self


    def getDateTimeFormat(self):
        """

        Returns:

            str: Format for DateTime when making DateTime:String conversions (default: yyyy-MM-dd HH:mm:ss)
        """
        return self.getOrDefault(self.dateTimeFormat)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.DataConversion"

    @staticmethod
    def _from_java(java_stage):
        module_name=DataConversion.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".DataConversion"
        return from_java(java_stage, module_name)
