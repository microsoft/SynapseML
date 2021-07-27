// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import scala.collection.JavaConverters._
import com.azure.ai.textanalytics.models._
import com.microsoft.ml.spark.core.schema.SparkBindings

object DetectLanguageResponseV4 extends SparkBindings[TAResponseV4[DetectedLanguageV4]]

object KeyPhraseResponseV4 extends SparkBindings[TAResponseV4[KeyphraseV4]]

object SentimentResponseV4 extends SparkBindings[TAResponseV4[SentimentScoredDocumentV4]]

case class TAResponseV4[T](result: Seq[Option[T]],
                           error: Seq[Option[TAErrorV4]],
                           statistics: Seq[Option[DocumentStatistics]],
                           modelVersion: Option[String])

case class DetectedLanguageV4(name: String, iso6391Name: String, confidenceScore: Double)

case class TAErrorV4(errorCode: String, errorMessage: String, target: String)

case class TAWarningV4(warningCode: String, message: String)

case class TextDocumentInputs(id: String, text: String)

case class TextAnalyticsRequestOptionsV4(modelVersion: String,
                                         includeStatistics: Boolean,
                                         disableServiceLogs: Boolean)

case class KeyphraseV4(keyPhrases: Seq[String], warnings: Seq[TAWarningV4])

case class SentimentConfidenceScoreV4(negative: Double, neutral: Double, positive: Double)

object SentimentConfidenceScoreV4 {
  def fromSDK(score: SentimentConfidenceScores): SentimentConfidenceScoreV4 = {
    SentimentConfidenceScoreV4(
      score.getNegative,
      score.getNeutral,
      score.getPositive)
  }
}

object SentimentScoredDocumentV4 {
  def fromSDK(doc: DocumentSentiment): SentimentScoredDocumentV4 = {
    SentimentScoredDocumentV4(
      doc.getSentiment.toString,
      SentimentConfidenceScoreV4.fromSDK(doc.getConfidenceScores),
      doc.getSentences.asScala.toSeq.map(sentenceSentiment =>
        SentimentSentenceV4.fromSDK(sentenceSentiment)),
      doc.getWarnings.asScala.toSeq.map(warnings =>
        WarningsV4(warnings.getMessage, warnings.getWarningCode.toString)))
  }
}

case class SentimentScoredDocumentV4(sentiment: String,
                                     confidenceScores: SentimentConfidenceScoreV4,
                                     sentences: Seq[SentimentSentenceV4],
                                     warnings: Seq[WarningsV4])

object SentimentSentenceV4 {
  def fromSDK(ss: SentenceSentiment): SentimentSentenceV4 = {
    SentimentSentenceV4(
      ss.getText,
      ss.getSentiment.toString,
      SentimentConfidenceScoreV4.fromSDK(ss.getConfidenceScores),
      Option(ss.getOpinions).map(sentmap =>
        sentmap.asScala.toList.map(op =>
          OpinionV4(
            TargetV4.fromSDK(op.getTarget),
            op.getAssessments.asScala.toList.map(assessment => AssessmentV4.fromSDK(assessment))
          )
        )
      ),
      ss.getOffset,
      ss.getLength)
  }
}

case class SentimentSentenceV4(text: String,
                               sentiment: String,
                               confidenceScores: SentimentConfidenceScoreV4,
                               opinion: Option[Seq[OpinionV4]],
                               offset: Int,
                               length: Int)

case class OpinionV4(target: TargetV4, assessment: Seq[AssessmentV4])

object TargetV4 {
  def fromSDK(target: TargetSentiment): TargetV4 = {
    TargetV4(
      target.getText,
      target.getSentiment.toString,
      SentimentConfidenceScoreV4.fromSDK(target.getConfidenceScores),
      target.getOffset,
      target.getLength)
  }
}

case class TargetV4(text: String,
                    sentiment: String,
                    confidenceScores: SentimentConfidenceScoreV4,
                    offset: Int,
                    length: Int)

object AssessmentV4 {
  def fromSDK(assess: AssessmentSentiment): AssessmentV4 = {
    AssessmentV4(
      assess.getText,
      assess.getSentiment.toString,
      SentimentConfidenceScoreV4.fromSDK(assess.getConfidenceScores),
      assess.isNegated,
      assess.getOffset,
      assess.getLength)
  }
}

case class AssessmentV4(text: String,
                        sentiment: String,
                        confidenceScores: SentimentConfidenceScoreV4,
                        isNegated: Boolean,
                        offset: Int,
                        length: Int)

case class WarningsV4(text: String, warningCode: String)
