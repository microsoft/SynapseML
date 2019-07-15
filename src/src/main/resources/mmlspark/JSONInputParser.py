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
class JSONInputParser(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        headers (dict): headers of the request (default: Map())
        inputCol (str): The name of the input column
        method (str): method to use for request, (PUT, POST, PATCH) (default: POST)
        outputCol (str): The name of the output column
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, headers={}, inputCol=None, method="POST", outputCol=None, url=None):
        super(JSONInputParser, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.JSONInputParser")
        self.headers = Param(self, "headers", "headers: headers of the request (default: Map())")
        self._setDefault(headers={})
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.method = Param(self, "method", "method: method to use for request, (PUT, POST, PATCH) (default: POST)")
        self._setDefault(method="POST")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        self.url = Param(self, "url", "url: Url of the service")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, headers={}, inputCol=None, method="POST", outputCol=None, url=None):
        """
        Set the (keyword only) parameters

        Args:

            headers (dict): headers of the request (default: Map())
            inputCol (str): The name of the input column
            method (str): method to use for request, (PUT, POST, PATCH) (default: POST)
            outputCol (str): The name of the output column
            url (str): Url of the service
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setHeaders(self, value):
        """

        Args:

            headers (dict): headers of the request (default: Map())

        """
        self._set(headers=value)
        return self


    def getHeaders(self):
        """

        Returns:

            dict: headers of the request (default: Map())
        """
        return self.getOrDefault(self.headers)


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


    def setMethod(self, value):
        """

        Args:

            method (str): method to use for request, (PUT, POST, PATCH) (default: POST)

        """
        self._set(method=value)
        return self


    def getMethod(self):
        """

        Returns:

            str: method to use for request, (PUT, POST, PATCH) (default: POST)
        """
        return self.getOrDefault(self.method)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column
        """
        return self.getOrDefault(self.outputCol)


    def setUrl(self, value):
        """

        Args:

            url (str): Url of the service

        """
        self._set(url=value)
        return self


    def getUrl(self):
        """

        Returns:

            str: Url of the service
        """
        return self.getOrDefault(self.url)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.JSONInputParser"

    @staticmethod
    def _from_java(java_stage):
        module_name=JSONInputParser.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".JSONInputParser"
        return from_java(java_stage, module_name)
