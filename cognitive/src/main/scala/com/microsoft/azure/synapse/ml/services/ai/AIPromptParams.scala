// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.contracts.HasInputCol
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.ai.AIJson.AnyFormat
import com.microsoft.azure.synapse.ml.services.openai.HasOpenAIModelParams
import org.apache.spark.ml.param.{IntParam, Param, ParamValidators}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsValue}

trait AIPromptParams extends HasOpenAIModelParams with HasInputCol {
  private val maximumOutputBatchSize = 100000

  val extraBody: ServiceParam[Map[String, Any]] = new AIJson.ObjectParam(
    this, "extraBody",
    "Provider-owned JSON body extension; cannot override explicit typed options or request identity.", "extraBody") {
    override def rValue(value: Either[Map[String, Any], String]): String = AIJson.rJsonValue(value)
  }

  def setExtraBody(value: Map[String, Any]): this.type = setExtraBody(AIJson.obj(AIJson.fromAny(value)))

  def setExtraBody(value: java.util.Map[String, Object]): this.type =
    setExtraBody(AIJson.obj(AIJson.fromAny(value)))

  def setExtraBody(value: JsObject): this.type =
    setScalarParam(extraBody, AIJson.orderedMap(AIJson.obj(AIJson.fromAny(value)).fields.toSeq.map {
      case (key, entry) => key -> AnyFormat.read(entry)
    }))

  def setExtraBody(value: String): this.type = setExtraBody(AIJson.objectString(value))
  def getExtraBody: Map[String, Any] = getScalarParam(extraBody)
  def setExtraBodyCol(value: String): this.type = setVectorParam(extraBody, value)
  def getExtraBodyCol: String = getVectorParam(extraBody)

  val questions: ServiceParam[Any] = new ServiceParam[Any](
    this, "questions", "Native questions: System One named object or Decisions ordered array.") {
    override def jsonDecode(json: String): Either[Any, String] = format.read(AIJson.parseServiceParam(json))
    override def rValue(value: Either[Any, String]): String = AIJson.rJsonValue(value)
  }

  def setQuestions(value: String): this.type = setQuestions(AIJson.parse(value))
  def setQuestions(value: JsValue): this.type = setScalarParam(questions, AnyFormat.read(AIJson.fromAny(value)))
  def setQuestions(value: Map[String, Any]): this.type = setQuestions(AIJson.fromAny(value))
  def setQuestions(value: Seq[Map[String, Any]]): this.type = setQuestions(AIJson.fromAny(value))
  def setQuestions(value: java.util.Map[String, Object]): this.type = setQuestions(AIJson.fromAny(value))
  def setQuestions(value: java.util.List[Object]): this.type = setQuestions(AIJson.fromAny(value))
  def getQuestions: Any = getScalarParam(questions)
  def setQuestionsCol(value: String): this.type = setVectorParam(questions, value)
  def getQuestionsCol: String = getVectorParam(questions)

  val stateCol: Param[String] = new Param[String](this, "stateCol", "System One state string column.")
  def setStateCol(value: String): this.type = set(stateCol, value)
  def getStateCol: String = $(stateCol)

  override val inputCol: Param[String] = new Param[String](this, "inputCol", "Input string column (text or JSON).")

  val inputMode: Param[String] = new Param[String](
    this, "inputMode", "Interpret native input as text or JSON.",
    ParamValidators.inArray(Array("text", "json")))
  def setInputMode(value: String): this.type = set(inputMode, value)
  def getInputMode: String = $(inputMode)
  def setStateMode(value: String): this.type = setInputMode(value)

  val text: ServiceParam[String] = new ServiceParam[String](this, "text", "Embedding input text.")
  def setText(value: String): this.type = setScalarParam(text, value)
  def getText: String = getScalarParam(text)
  def setTextCol(value: String): this.type = setVectorParam(text, value)
  def getTextCol: String = getVectorParam(text)

  val dimensions: ServiceParam[Int] = new ServiceParam[Int](
    this, "dimensions", "Embedding dimensions; must be a positive integer.")
  def setDimensions(value: Int): this.type = setScalarParam(dimensions, value)
  def getDimensions: Int = getScalarParam(dimensions)
  def setDimensionsCol(value: String): this.type = setVectorParam(dimensions, value)
  def getDimensionsCol: String = getVectorParam(dimensions)

  override val safetyIdentifier: ServiceParam[String] = new ServiceParam[String](
    this, "safetyIdentifier", "Decisions safety_identifier.") {
    override val payloadName: String = "safety_identifier"
  }
  override def setSafetyIdentifier(value: String): this.type = setScalarParam(safetyIdentifier, value)
  override def getSafetyIdentifier: String = getScalarParam(safetyIdentifier)
  override def setSafetyIdentifierCol(value: String): this.type = setVectorParam(safetyIdentifier, value)
  override def getSafetyIdentifierCol: String = getVectorParam(safetyIdentifier)

  val rawResponseCol: Param[String] = new Param[String](
    this, "rawResponseCol", "Complete response JSON, including unknown fields; never includes HTTP headers.")
  def setRawResponseCol(value: String): this.type = set(rawResponseCol, value)
  def getRawResponseCol: String = $(rawResponseCol)

  val outputPath: Param[String] = new Param[String](
    this, "outputPath", "Fresh caller-owned Delta run directory; enables eager, resumable input and result storage.")
  def setOutputPath(value: String): this.type = set(outputPath, value)
  def getOutputPath: String = $(outputPath)

  val outputBatchSize: IntParam = new IntParam(
    this, "outputBatchSize", "Maximum rows per durable Delta batch.",
    ParamValidators.inRange(1, maximumOutputBatchSize, lowerInclusive = true, upperInclusive = true))
  def setOutputBatchSize(value: Int): this.type = set(outputBatchSize, value)
  def getOutputBatchSize: Int = $(outputBatchSize)

  setDefault(inputMode -> "text", outputBatchSize -> 1000)
}
