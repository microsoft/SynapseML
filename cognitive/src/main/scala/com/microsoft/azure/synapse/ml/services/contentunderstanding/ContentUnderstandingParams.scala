// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.HasServiceParams
import org.apache.spark.ml.param.{IntParam, Param, ParamValidators}
import spray.json.DefaultJsonProtocol._

trait ContentUnderstandingParams extends HasServiceParams {

  val analyzerId = new ServiceParam[String](this, "analyzerId",
    "Prebuilt or explicitly provisioned custom analyzer identifier.")

  def setAnalyzerId(value: String): this.type = setScalarParam(analyzerId, value)
  def setAnalyzerIdCol(value: String): this.type = setVectorParam(analyzerId, value)
  def getAnalyzerId: String = getScalarParam(analyzerId)
  def getAnalyzerIdCol: String = getVectorParam(analyzerId)

  val documentUrl = new ServiceParam[String](this, "documentUrl",
    "URL the service can read. Configure exactly one of documentUrl and documentBytes.")

  def setDocumentUrl(value: String): this.type = setScalarParam(documentUrl, value)
  def setDocumentUrlCol(value: String): this.type = setVectorParam(documentUrl, value)
  def getDocumentUrl: String = getScalarParam(documentUrl)
  def getDocumentUrlCol: String = getVectorParam(documentUrl)

  val documentBytes: ServiceParam[Array[Byte]] = new ServiceParam[Array[Byte]](this, "documentBytes",
    "Document bytes, sent as inputs[].data using base64. Use BinaryType for a column.") {
    override def pyValue(value: Either[Array[Byte], String]): String = value match {
      case Left(bytes) =>
        val unsignedByteMask = 0xff
        bytes.map(_ & unsignedByteMask).mkString("bytearray([", ", ", "])")
      case Right(column) => super.pyValue(Right(column))
    }
  }

  def setDocumentBytes(value: Array[Byte]): this.type = setScalarParam(documentBytes, value)
  def setDocumentBytesCol(value: String): this.type = setVectorParam(documentBytes, value)
  def getDocumentBytes: Array[Byte] = getScalarParam(documentBytes)
  def getDocumentBytesCol: String = getVectorParam(documentBytes)

  val range = new ServiceParam[String](this, "range",
    "Optional service range: 1-based document pages, or integer milliseconds for audio/video.")

  def setRange(value: String): this.type = setScalarParam(range, value)
  def setRangeCol(value: String): this.type = setVectorParam(range, value)
  def getRange: String = getScalarParam(range)
  def getRangeCol: String = getVectorParam(range)

  val mimeType = new ServiceParam[String](this, "mimeType", "Optional MIME type of the document.")

  def setMimeType(value: String): this.type = setScalarParam(mimeType, value)
  def setMimeTypeCol(value: String): this.type = setVectorParam(mimeType, value)
  def getMimeType: String = getScalarParam(mimeType)
  def getMimeTypeCol: String = getVectorParam(mimeType)

  val documentName = new ServiceParam[String](this, "documentName", "Optional input document name.")

  def setDocumentName(value: String): this.type = setScalarParam(documentName, value)
  def setDocumentNameCol(value: String): this.type = setVectorParam(documentName, value)
  def getDocumentName: String = getScalarParam(documentName)
  def getDocumentNameCol: String = getVectorParam(documentName)

  val modelDeployments = new ServiceParam[Map[String, String]](this, "modelDeployments",
    "Per-request model-name or prebuilt-alias to deployment-name mapping; does not modify resource defaults.")

  def setModelDeployments(value: Map[String, String]): this.type = setScalarParam(modelDeployments, value)
  def setModelDeploymentsCol(value: String): this.type = setVectorParam(modelDeployments, value)
  def getModelDeployments: Map[String, String] = getScalarParam(modelDeployments)
  def getModelDeploymentsCol: String = getVectorParam(modelDeployments)

  val stringEncoding = new ServiceParam[String](this, "stringEncoding",
    "String offset encoding, for example codePoint, utf16, or utf8.", isURLParam = true)

  def setStringEncoding(value: String): this.type = setScalarParam(stringEncoding, value)
  def setStringEncodingCol(value: String): this.type = setVectorParam(stringEncoding, value)
  def getStringEncoding: String = getScalarParam(stringEncoding)
  def getStringEncodingCol: String = getVectorParam(stringEncoding)

  val processingLocation = new ServiceParam[String](this, "processingLocation",
    "Service processing location, for example geography, dataZone, or global.", isURLParam = true)

  def setProcessingLocation(value: String): this.type = setScalarParam(processingLocation, value)
  def setProcessingLocationCol(value: String): this.type = setVectorParam(processingLocation, value)
  def getProcessingLocation: String = getScalarParam(processingLocation)
  def getProcessingLocationCol: String = getVectorParam(processingLocation)

  val operationLocation = new ServiceParam[String](this, "operationLocation",
    "Previously accepted operation URL. Used only in poll mode and restricted to the configured endpoint.")

  def setOperationLocation(value: String): this.type = setScalarParam(operationLocation, value)
  def setOperationLocationCol(value: String): this.type = setVectorParam(operationLocation, value)
  def getOperationLocation: String = getScalarParam(operationLocation)
  def getOperationLocationCol: String = getVectorParam(operationLocation)

  val operationMode = new Param[String](this, "operationMode",
    "analyze submits and polls; submit only submits; poll resumes an operation without document input.",
    ParamValidators.inArray(Array("analyze", "submit", "poll")))

  def setOperationMode(value: String): this.type = set(operationMode, value)
  def getOperationMode: String = $(operationMode)

  val maxPollAttempts = new IntParam(this, "maxPollAttempts",
    "Maximum GET attempts, including transient failures. Exhaustion preserves the last running operation.",
    ParamValidators.gt(0))

  def setMaxPollAttempts(value: Int): this.type = set(maxPollAttempts, value)
  def getMaxPollAttempts: Int = $(maxPollAttempts)

  val pollingDelay = new IntParam(this, "pollingDelay",
    "Milliseconds between polls when Retry-After is absent. Zero is useful for local testing.",
    ParamValidators.gtEq(0))

  def setPollingDelay(value: Int): this.type = set(pollingDelay, value)
  def getPollingDelay: Int = $(pollingDelay)

  val maxResponseBytes = new IntParam(this, "maxResponseBytes",
    "Maximum bytes per HTTP response before JSON parsing. Split large documents into explicit ranges.",
    ParamValidators.gt(0))

  def setMaxResponseBytes(value: Int): this.type = set(maxResponseBytes, value)
  def getMaxResponseBytes: Int = $(maxResponseBytes)

  setDefault(
    analyzerId -> Left("prebuilt-read"),
    operationMode -> "analyze",
    maxPollAttempts -> 120,
    pollingDelay -> 1000,
    maxResponseBytes -> 32 * 1024 * 1024)
}

private[contentunderstanding] trait ContentUnderstandingPython extends Wrappable {
  this: ContentUnderstanding =>

  override protected def pyParamSetter(param: Param[_]): String = {
    if (param.name == "documentBytes") {
      """
        |def setDocumentBytes(self, value):
        |    '''Set bytes, bytearray, or a sequence of unsigned byte values.'''
        |    self._java_obj = self._java_obj.setDocumentBytes(bytearray(value))
        |    return self
        |
        |def setDocumentBytesCol(self, value):
        |    self._java_obj = self._java_obj.setDocumentBytesCol(value)
        |    return self
        |""".stripMargin
    } else {
      super.pyParamSetter(param)
    }
  }

  override protected def pySetParamsFunc: String = {
    """
      |def setParams(self, **kwargs):
      |    '''Set parameters using the same JVM setters as the constructor.'''
      |    for name, value in kwargs.items():
      |        if value is not None:
      |            getattr(self, "set" + name[0].upper() + name[1:])(value)
      |    return self
      |""".stripMargin
  }

  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """
      |def _transfer_params_from_java(self):
      |    from pyspark.ml.common import _java2py
      |    sc = SparkContext._active_spark_context
      |    # JVM-backed service params must not become None scalars or pickle-decoded document bytes.
      |    for param in self.params:
      |        if param.doc.startswith("ServiceParam:"):
      |            self._paramMap.pop(param, None)
      |        elif self._java_obj.hasParam(param.name):
      |            java_param = self._java_obj.getParam(param.name)
      |            if self._java_obj.isSet(java_param):
      |                self._set(**{param.name: _java2py(sc, self._java_obj.getOrDefault(java_param))})
      |
      |def _transfer_params_to_java(self):
      |    # Use the generated setters for ParamMap/copy values, including bytes and model maps.
      |    for param in list(self._paramMap):
      |        if param.doc.startswith("ServiceParam:"):
      |            value = self._paramMap.pop(param)
      |            if value is not None:
      |                getattr(self, "set" + param.name[0].upper() + param.name[1:])(value)
      |    super()._transfer_params_to_java()
      |
      |def clear(self, param):
      |    param = self._resolveParam(param)
      |    self._java_obj.clear(self._java_obj.getParam(param.name))
      |    return super().clear(param)
      |
      |def createAnalyzer(self, definition: "dict | str", allowReplace: bool = False) -> str:
      |    '''Explicit driver-only provisioning. Never changes resource defaults.'''
      |    import json
      |    self._transfer_params_to_java()
      |    payload = json.dumps(definition) if isinstance(definition, dict) else definition
      |    return self._java_obj.createAnalyzer(payload, allowReplace)
      |
      |def getAnalyzer(self) -> str:
      |    '''Get the current scalar analyzer definition from the service.'''
      |    self._transfer_params_to_java()
      |    return self._java_obj.getAnalyzer()
      |""".stripMargin
  }
}
