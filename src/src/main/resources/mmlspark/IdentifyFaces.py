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
class IdentifyFaces(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        confidenceThreshold (object): Optional parameter.Customized identification confidence threshold, in the range of [0, 1].Advanced user can tweak this value to override defaultinternal threshold for better precision on their scenario data.Note there is no guarantee of this threshold value workingon other data and after algorithm updates.
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        faceIds (object): Array of query faces faceIds, created by the Face - Detect. Each of the faces are identified independently. The valid number of faceIds is between [1, 10].
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        largePersonGroupId (object): largePersonGroupId of the target large person group, created by LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        maxNumOfCandidatesReturned (object): The range of maxNumOfCandidatesReturned is between 1 and 100 (default is 10).
        outputCol (str): The name of the output column (default: [self.uid]_output)
        personGroupId (object): personGroupId of the target person group, created by PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, concurrency=1, concurrentTimeout=100.0, confidenceThreshold=None, errorCol=None, faceIds=None, handler=None, largePersonGroupId=None, maxNumOfCandidatesReturned=None, outputCol=None, personGroupId=None, subscriptionKey=None, timeout=60.0, url=None):
        super(IdentifyFaces, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.IdentifyFaces")
        self._cache = {}
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.confidenceThreshold = Param(self, "confidenceThreshold", "confidenceThreshold: Optional parameter.Customized identification confidence threshold, in the range of [0, 1].Advanced user can tweak this value to override defaultinternal threshold for better precision on their scenario data.Note there is no guarantee of this threshold value workingon other data and after algorithm updates.")
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.faceIds = Param(self, "faceIds", "faceIds: Array of query faces faceIds, created by the Face - Detect. Each of the faces are identified independently. The valid number of faceIds is between [1, 10].")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.largePersonGroupId = Param(self, "largePersonGroupId", "largePersonGroupId: largePersonGroupId of the target large person group, created by LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.")
        self.maxNumOfCandidatesReturned = Param(self, "maxNumOfCandidatesReturned", "maxNumOfCandidatesReturned: The range of maxNumOfCandidatesReturned is between 1 and 100 (default is 10).")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.personGroupId = Param(self, "personGroupId", "personGroupId: personGroupId of the target person group, created by PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.")
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
    def setParams(self, concurrency=1, concurrentTimeout=100.0, confidenceThreshold=None, errorCol=None, faceIds=None, handler=None, largePersonGroupId=None, maxNumOfCandidatesReturned=None, outputCol=None, personGroupId=None, subscriptionKey=None, timeout=60.0, url=None):
        """
        Set the (keyword only) parameters

        Args:

            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            confidenceThreshold (object): Optional parameter.Customized identification confidence threshold, in the range of [0, 1].Advanced user can tweak this value to override defaultinternal threshold for better precision on their scenario data.Note there is no guarantee of this threshold value workingon other data and after algorithm updates.
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            faceIds (object): Array of query faces faceIds, created by the Face - Detect. Each of the faces are identified independently. The valid number of faceIds is between [1, 10].
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            largePersonGroupId (object): largePersonGroupId of the target large person group, created by LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
            maxNumOfCandidatesReturned (object): The range of maxNumOfCandidatesReturned is between 1 and 100 (default is 10).
            outputCol (str): The name of the output column (default: [self.uid]_output)
            personGroupId (object): personGroupId of the target person group, created by PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
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


    def setConfidenceThreshold(self, value):
        """

        Args:

            confidenceThreshold (object): Optional parameter.Customized identification confidence threshold, in the range of [0, 1].Advanced user can tweak this value to override defaultinternal threshold for better precision on their scenario data.Note there is no guarantee of this threshold value workingon other data and after algorithm updates.

        """
        self._java_obj = self._java_obj.setConfidenceThreshold(value)
        return self


    def setConfidenceThresholdCol(self, value):
        """

        Args:

            confidenceThreshold (object): Optional parameter.Customized identification confidence threshold, in the range of [0, 1].Advanced user can tweak this value to override defaultinternal threshold for better precision on their scenario data.Note there is no guarantee of this threshold value workingon other data and after algorithm updates.

        """
        self._java_obj = self._java_obj.setConfidenceThresholdCol(value)
        return self




    def getConfidenceThreshold(self):
        """

        Returns:

            object: Optional parameter.Customized identification confidence threshold, in the range of [0, 1].Advanced user can tweak this value to override defaultinternal threshold for better precision on their scenario data.Note there is no guarantee of this threshold value workingon other data and after algorithm updates.
        """
        return self._cache.get("confidenceThreshold", None)


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


    def setFaceIds(self, value):
        """

        Args:

            faceIds (object): Array of query faces faceIds, created by the Face - Detect. Each of the faces are identified independently. The valid number of faceIds is between [1, 10].

        """
        self._java_obj = self._java_obj.setFaceIds(value)
        return self


    def setFaceIdsCol(self, value):
        """

        Args:

            faceIds (object): Array of query faces faceIds, created by the Face - Detect. Each of the faces are identified independently. The valid number of faceIds is between [1, 10].

        """
        self._java_obj = self._java_obj.setFaceIdsCol(value)
        return self




    def getFaceIds(self):
        """

        Returns:

            object: Array of query faces faceIds, created by the Face - Detect. Each of the faces are identified independently. The valid number of faceIds is between [1, 10].
        """
        return self._cache.get("faceIds", None)


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

            largePersonGroupId (object): largePersonGroupId of the target large person group, created by LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setLargePersonGroupId(value)
        return self


    def setLargePersonGroupIdCol(self, value):
        """

        Args:

            largePersonGroupId (object): largePersonGroupId of the target large person group, created by LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setLargePersonGroupIdCol(value)
        return self




    def getLargePersonGroupId(self):
        """

        Returns:

            object: largePersonGroupId of the target large person group, created by LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        """
        return self._cache.get("largePersonGroupId", None)


    def setMaxNumOfCandidatesReturned(self, value):
        """

        Args:

            maxNumOfCandidatesReturned (object): The range of maxNumOfCandidatesReturned is between 1 and 100 (default is 10).

        """
        self._java_obj = self._java_obj.setMaxNumOfCandidatesReturned(value)
        return self


    def setMaxNumOfCandidatesReturnedCol(self, value):
        """

        Args:

            maxNumOfCandidatesReturned (object): The range of maxNumOfCandidatesReturned is between 1 and 100 (default is 10).

        """
        self._java_obj = self._java_obj.setMaxNumOfCandidatesReturnedCol(value)
        return self




    def getMaxNumOfCandidatesReturned(self):
        """

        Returns:

            object: The range of maxNumOfCandidatesReturned is between 1 and 100 (default is 10).
        """
        return self._cache.get("maxNumOfCandidatesReturned", None)


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

            personGroupId (object): personGroupId of the target person group, created by PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setPersonGroupId(value)
        return self


    def setPersonGroupIdCol(self, value):
        """

        Args:

            personGroupId (object): personGroupId of the target person group, created by PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setPersonGroupIdCol(value)
        return self




    def getPersonGroupId(self):
        """

        Returns:

            object: personGroupId of the target person group, created by PersonGroup - Create. Parameter personGroupId and largePersonGroupId should not be provided at the same time.
        """
        return self._cache.get("personGroupId", None)


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
        return "com.microsoft.ml.spark.IdentifyFaces"

    @staticmethod
    def _from_java(java_stage):
        module_name=IdentifyFaces.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".IdentifyFaces"
        return from_java(java_stage, module_name)
