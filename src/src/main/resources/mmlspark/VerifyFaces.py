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
class VerifyFaces(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        faceId (object): faceId of the face, comes from Face - Detect.
        faceId1 (object): faceId of one face, comes from Face - Detect.
        faceId2 (object): faceId of another face, comes from Face - Detect.
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        largePersonGroupId (object): Using existing largePersonGroupId and personId for fast ading a specified person. largePersonGroupId is created in LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        outputCol (str): The name of the output column (default: [self.uid]_output)
        personGroupId (object): Using existing personGroupId and personId for fast loading a specified person. personGroupId is created in PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        personId (object): Specify a certain person in a person group or a large person group. personId is created in PersonGroup Person - Create or LargePersonGroup Person - Create.
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, faceId=None, faceId1=None, faceId2=None, handler=None, largePersonGroupId=None, outputCol=None, personGroupId=None, personId=None, subscriptionKey=None, timeout=60.0, url=None):
        super(VerifyFaces, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.VerifyFaces")
        self._cache = {}
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.faceId = Param(self, "faceId", "faceId: faceId of the face, comes from Face - Detect.")
        self.faceId1 = Param(self, "faceId1", "faceId1: faceId of one face, comes from Face - Detect.")
        self.faceId2 = Param(self, "faceId2", "faceId2: faceId of another face, comes from Face - Detect.")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.largePersonGroupId = Param(self, "largePersonGroupId", "largePersonGroupId: Using existing largePersonGroupId and personId for fast ading a specified person. largePersonGroupId is created in LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.personGroupId = Param(self, "personGroupId", "personGroupId: Using existing personGroupId and personId for fast loading a specified person. personGroupId is created in PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.")
        self.personId = Param(self, "personId", "personId: Specify a certain person in a person group or a large person group. personId is created in PersonGroup Person - Create or LargePersonGroup Person - Create.")
        self.subscriptionKey = Param(self, "subscriptionKey", "subscriptionKey: the API key to use")
        self.timeout = Param(self, "timeout", "timeout: number of seconds to wait before closing the connection (default: 60.0)")
        self._setDefault(timeout=60.0)
        self.url = Param(self, "url", "url: Url of the service")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, faceId=None, faceId1=None, faceId2=None, handler=None, largePersonGroupId=None, outputCol=None, personGroupId=None, personId=None, subscriptionKey=None, timeout=60.0, url=None):
        """
        Set the (keyword only) parameters

        Args:

            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            faceId (object): faceId of the face, comes from Face - Detect.
            faceId1 (object): faceId of one face, comes from Face - Detect.
            faceId2 (object): faceId of another face, comes from Face - Detect.
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            largePersonGroupId (object): Using existing largePersonGroupId and personId for fast ading a specified person. largePersonGroupId is created in LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
            outputCol (str): The name of the output column (default: [self.uid]_output)
            personGroupId (object): Using existing personGroupId and personId for fast loading a specified person. personGroupId is created in PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
            personId (object): Specify a certain person in a person group or a large person group. personId is created in PersonGroup Person - Create or LargePersonGroup Person - Create.
            subscriptionKey (object): the API key to use
            timeout (double): number of seconds to wait before closing the connection (default: 60.0)
            url (str): Url of the service
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setConcurrency(self, value):
        """

        Args:

            concurrency (int): max number of concurrent calls (default: 1)

        """
        self._set(concurrency=value)
        return self


    def getConcurrency(self):
        """

        Returns:

            int: max number of concurrent calls (default: 1)
        """
        return self.getOrDefault(self.concurrency)


    def setConcurrentTimeout(self, value):
        """

        Args:

            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)

        """
        self._set(concurrentTimeout=value)
        return self


    def getConcurrentTimeout(self):
        """

        Returns:

            double: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        """
        return self.getOrDefault(self.concurrentTimeout)


    def setErrorCol(self, value):
        """

        Args:

            errorCol (str): column to hold http errors (default: [self.uid]_error)

        """
        self._set(errorCol=value)
        return self


    def getErrorCol(self):
        """

        Returns:

            str: column to hold http errors (default: [self.uid]_error)
        """
        return self.getOrDefault(self.errorCol)


    def setFaceId(self, value):
        """

        Args:

            faceId (object): faceId of the face, comes from Face - Detect.

        """
        self._java_obj = self._java_obj.setFaceId(value)
        return self


    def setFaceIdCol(self, value):
        """

        Args:

            faceId (object): faceId of the face, comes from Face - Detect.

        """
        self._java_obj = self._java_obj.setFaceIdCol(value)
        return self




    def getFaceId(self):
        """

        Returns:

            object: faceId of the face, comes from Face - Detect.
        """
        return self._cache.get("faceId", None)


    def setFaceId1(self, value):
        """

        Args:

            faceId1 (object): faceId of one face, comes from Face - Detect.

        """
        self._java_obj = self._java_obj.setFaceId1(value)
        return self


    def setFaceId1Col(self, value):
        """

        Args:

            faceId1 (object): faceId of one face, comes from Face - Detect.

        """
        self._java_obj = self._java_obj.setFaceId1Col(value)
        return self




    def getFaceId1(self):
        """

        Returns:

            object: faceId of one face, comes from Face - Detect.
        """
        return self._cache.get("faceId1", None)


    def setFaceId2(self, value):
        """

        Args:

            faceId2 (object): faceId of another face, comes from Face - Detect.

        """
        self._java_obj = self._java_obj.setFaceId2(value)
        return self


    def setFaceId2Col(self, value):
        """

        Args:

            faceId2 (object): faceId of another face, comes from Face - Detect.

        """
        self._java_obj = self._java_obj.setFaceId2Col(value)
        return self




    def getFaceId2(self):
        """

        Returns:

            object: faceId of another face, comes from Face - Detect.
        """
        return self._cache.get("faceId2", None)


    def setHandler(self, value):
        """

        Args:

            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))

        """
        self._set(handler=value)
        return self


    def getHandler(self):
        """

        Returns:

            object: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        """
        return self._cache.get("handler", None)


    def setLargePersonGroupId(self, value):
        """

        Args:

            largePersonGroupId (object): Using existing largePersonGroupId and personId for fast ading a specified person. largePersonGroupId is created in LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setLargePersonGroupId(value)
        return self


    def setLargePersonGroupIdCol(self, value):
        """

        Args:

            largePersonGroupId (object): Using existing largePersonGroupId and personId for fast ading a specified person. largePersonGroupId is created in LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setLargePersonGroupIdCol(value)
        return self




    def getLargePersonGroupId(self):
        """

        Returns:

            object: Using existing largePersonGroupId and personId for fast ading a specified person. largePersonGroupId is created in LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        """
        return self._cache.get("largePersonGroupId", None)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column (default: [self.uid]_output)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column (default: [self.uid]_output)
        """
        return self.getOrDefault(self.outputCol)


    def setPersonGroupId(self, value):
        """

        Args:

            personGroupId (object): Using existing personGroupId and personId for fast loading a specified person. personGroupId is created in PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setPersonGroupId(value)
        return self


    def setPersonGroupIdCol(self, value):
        """

        Args:

            personGroupId (object): Using existing personGroupId and personId for fast loading a specified person. personGroupId is created in PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setPersonGroupIdCol(value)
        return self




    def getPersonGroupId(self):
        """

        Returns:

            object: Using existing personGroupId and personId for fast loading a specified person. personGroupId is created in PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        """
        return self._cache.get("personGroupId", None)


    def setPersonId(self, value):
        """

        Args:

            personId (object): Specify a certain person in a person group or a large person group. personId is created in PersonGroup Person - Create or LargePersonGroup Person - Create.

        """
        self._java_obj = self._java_obj.setPersonId(value)
        return self


    def setPersonIdCol(self, value):
        """

        Args:

            personId (object): Specify a certain person in a person group or a large person group. personId is created in PersonGroup Person - Create or LargePersonGroup Person - Create.

        """
        self._java_obj = self._java_obj.setPersonIdCol(value)
        return self




    def getPersonId(self):
        """

        Returns:

            object: Specify a certain person in a person group or a large person group. personId is created in PersonGroup Person - Create or LargePersonGroup Person - Create.
        """
        return self._cache.get("personId", None)


    def setSubscriptionKey(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKey(value)
        return self


    def setSubscriptionKeyCol(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKeyCol(value)
        return self




    def getSubscriptionKey(self):
        """

        Returns:

            object: the API key to use
        """
        return self._cache.get("subscriptionKey", None)


    def setTimeout(self, value):
        """

        Args:

            timeout (double): number of seconds to wait before closing the connection (default: 60.0)

        """
        self._set(timeout=value)
        return self


    def getTimeout(self):
        """

        Returns:

            double: number of seconds to wait before closing the connection (default: 60.0)
        """
        return self.getOrDefault(self.timeout)


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
        return "com.microsoft.ml.spark.VerifyFaces"

    @staticmethod
    def _from_java(java_stage):
        module_name=VerifyFaces.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".VerifyFaces"
        return from_java(java_stage, module_name)
