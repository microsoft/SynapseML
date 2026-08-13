// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.io.http.{
  EntityData, HTTPResponseData, ProtocolVersionData, StatusLineData
}
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.scalatest.funsuite.AnyFunSuite
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.net.URI

private[language] class TestableAnalyzeText extends AnalyzeText {
  def buildEntity(row: Row): StringEntity = prepareEntity(row).get.asInstanceOf[StringEntity]

  def outputSchema: DataType = responseDataType
}

private[language] class TestableAnalyzeTextLRO extends AnalyzeTextLongRunningOperations {
  def buildEntity(row: Row): StringEntity = prepareEntity(row).get.asInstanceOf[StringEntity]

  def outputSchema: DataType = responseDataType

  def buildPollingURI(uri: URI): URI = modifyPollingURI(uri)
}

class AnalyzeTextCoreOfflineSuite extends AnyFunSuite {

  private def parseEntity(entity: StringEntity): JsObject = {
    EntityUtils.toString(entity, "UTF-8").parseJson.asJsObject
  }

  private def responseWithEntity(json: String): HTTPResponseData = {
    HTTPResponseData(
      headers = Array.empty,
      entity = Some(EntityData(
        json.getBytes("UTF-8"),
        None,
        None,
        None,
        isChunked = false,
        isRepeatable = true,
        isStreaming = false)),
      statusLine = StatusLineData(ProtocolVersionData("HTTP", 1, 1), 200, "OK"),
      locale = "en_US")
  }

  test("local parameter validation rejects invalid values") {
    intercept[IllegalArgumentException] {
      new AnalyzeText().setKind("UnknownTask")
    }
    intercept[IllegalArgumentException] {
      new AnalyzeText().setStringIndexType("invalid-index")
    }
    intercept[IllegalArgumentException] {
      new AnalyzeTextLongRunningOperations().setSentenceCount(0)
    }
    intercept[IllegalArgumentException] {
      new AnalyzeTextLongRunningOperations().setSortBy("Score")
    }
    intercept[IllegalArgumentException] {
      new AnalyzeTextLongRunningOperations().setSummaryLength("tiny")
    }
  }

  test("analyze text request-building is deterministic for language detection") {
    val transformer = new TestableAnalyzeText()
      .setKind("LanguageDetection")
      .setText(Seq("Hello", ""))
      .setCountryHint("US")
      .setModelVersion("2024-10-01")
      .setLoggingOptOut(true)

    val payload = parseEntity(transformer.buildEntity(Row.empty))
    assert(payload.fields("kind").convertTo[String] == "LanguageDetection")

    val analysisInput = payload.fields("analysisInput").asJsObject
    val JsArray(documents) = analysisInput.fields("documents")
    assert(documents.length == 2)
    assert(documents.head.asJsObject.fields("countryHint").convertTo[String] == "US")
    assert(documents(1).asJsObject.fields("countryHint").convertTo[String] == "US")
    assert(documents(1).asJsObject.fields("text").convertTo[String] == "")

    val params = payload.fields("parameters").asJsObject
    assert(params.fields("loggingOptOut").convertTo[Boolean])
    assert(params.fields("modelVersion").convertTo[String] == "2024-10-01")
  }

  test("schema behavior follows selected task kind") {
    val analyze = new TestableAnalyzeText().setKind("EntityLinking")
    assert(analyze.outputSchema == EntityLinkingResponse.schema)
    analyze.setKind("SentimentAnalysis")
    assert(analyze.outputSchema == SentimentResponse.schema)

    val lro = new TestableAnalyzeTextLRO()
      .setKind(AnalysisTaskKind.CustomMultiLabelClassification)
    assert(lro.outputSchema == CustomLabelJobState.schema)
    lro.setKind(AnalysisTaskKind.Healthcare)
    assert(lro.outputSchema == HealthcareJobState.schema)
  }

  test("lro request-building captures helper options deterministically") {
    val transformer = new TestableAnalyzeTextLRO()
      .setKind(AnalysisTaskKind.EntityRecognition)
      .setText(Seq("John Doe"))
      .setLanguage("en")
      .setModelVersion("2024-06-01")
      .setStringIndexType("UnicodeCodePoint")
      .setLoggingOptOut(true)
      .setInclusionList(Seq("Person"))
      .setOverlapPolicy("allowOverlap")
      .setExcludeNormalizedValues(true)

    val payload = parseEntity(transformer.buildEntity(Row.empty))
    val analysisInput = payload.fields("analysisInput").asJsObject
    val JsArray(documents) = analysisInput.fields("documents")
    assert(documents.head.asJsObject.fields("language").convertTo[String] == "en")

    val JsArray(tasks) = payload.fields("tasks")
    val parameters = tasks.head.asJsObject.fields("parameters").asJsObject
    assert(parameters.fields("modelVersion").convertTo[String] == "2024-06-01")
    assert(parameters.fields("stringIndexType").convertTo[String] == "UnicodeCodePoint")
    assert(parameters.fields("inclusionList").convertTo[Seq[String]] == Seq("Person"))
    assert(
      parameters.fields("overlapPolicy").asJsObject.fields("policyKind").convertTo[String] == "allowOverlap")
    assert(
      parameters.fields("inferenceOptions").asJsObject.fields("excludeNormalizedValues").convertTo[Boolean])
  }

  test("helper logic is deterministic for polling uri, kind mapping, and response rewrite") {
    assert(AnalysisTaskKind.getKindFromString("Healthcare") == AnalysisTaskKind.Healthcare)
    val ex = intercept[IllegalArgumentException] {
      AnalysisTaskKind.getKindFromString("Nope")
    }
    assert(ex.getMessage.contains("Invalid kind"))

    val uri = new URI("https://example.test/jobs/1?api-version=2023-04-01")
    val noStats = new TestableAnalyzeTextLRO()
    assert(noStats.buildPollingURI(uri) == uri)
    noStats.setShowStats(true)
    assert(noStats.buildPollingURI(uri).toString.endsWith("&showStats=true"))

    val raw = responseWithEntity("""{"class":"Top","nested":{"class":"Secondary"}}""")
    val rewritten = new TestableAnalyzeTextLRO()
      .setKind(AnalysisTaskKind.CustomSingleLabelClassification)
      .modifyResponse(Some(raw))
      .get
    val rewrittenBody = new String(rewritten.entity.get.content, "UTF-8")
    assert(rewrittenBody.contains("\"classifications\":\"Top\""))
    assert(rewrittenBody.contains("\"classifications\":\"Secondary\""))
    assert(!rewrittenBody.contains("\"class\":"))
  }
}
