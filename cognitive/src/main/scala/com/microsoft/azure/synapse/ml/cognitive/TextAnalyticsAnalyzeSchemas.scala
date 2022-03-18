// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import scala.collection.JavaConverters._

// Text Analytics /analyze endpoint schemas

case class TAAnalyzeAnalysisInput(documents: Seq[TADocument])

object TAAnalyzeAnalysisInput extends SparkBindings[TAAnalyzeAnalysisInput]

case class TAAnalyzeTask(parameters: Map[String, String]) {
  def this(parameters: java.util.HashMap[String, String]) {
    this(parameters.asScala.toMap)
  }
}

object TAAnalyzeTask extends SparkBindings[TAAnalyzeTask]

case class TAAnalyzeTasks(entityRecognitionTasks: Seq[TAAnalyzeTask],
                          entityLinkingTasks: Seq[TAAnalyzeTask],
                          entityRecognitionPiiTasks: Seq[TAAnalyzeTask],
                          keyPhraseExtractionTasks: Seq[TAAnalyzeTask],
                          sentimentAnalysisTasks: Seq[TAAnalyzeTask])

object TAAnalyzeTasks extends SparkBindings[TAAnalyzeTasks]

case class TAAnalyzeRequest(displayName: String,
                            analysisInput: TAAnalyzeAnalysisInput,
                            tasks: TAAnalyzeTasks)

object TAAnalyzeRequest extends SparkBindings[TAAnalyzeRequest]


case class TAAnalyzeResponseTaskResults[T](documents: Seq[T],
                                           errors: Seq[TAError],
                                           modelVersion: String)

case class TAAnalyzeResponseTask[T](state: String,
                                    results: TAAnalyzeResponseTaskResults[T])

case class TAAnalyzeResponseTasks(completed: Int,
                                  failed: Int,
                                  inProgress: Int,
                                  total: Int,
                                  entityRecognitionTasks: Option[Seq[TAAnalyzeResponseTask[NERDocV3]]],
                                  entityLinkingTasks: Option[Seq[TAAnalyzeResponseTask[DetectEntitiesScoreV3]]],
                                  entityRecognitionPiiTasks: Option[Seq[TAAnalyzeResponseTask[PIIDocV3]]],
                                  keyPhraseExtractionTasks: Option[Seq[TAAnalyzeResponseTask[KeyPhraseScoreV3]]],
                                  sentimentAnalysisTasks: Option[Seq[TAAnalyzeResponseTask[SentimentScoredDocumentV3]]]
                                 )

// API call response
case class TAAnalyzeResponse(status: String,
                             errors: Option[Seq[TAError]],
                             displayName: String,
                             tasks: TAAnalyzeResponseTasks)

object TAAnalyzeResponse extends SparkBindings[TAAnalyzeResponse]

case class TAAnalyzeResultTaskResults[T](result: Option[T],
                                         error: Option[TAError])

case class TAAnalyzeResult(entityRecognition: Option[Seq[TAAnalyzeResultTaskResults[NERDocV3]]],
                           entityLinking: Option[Seq[TAAnalyzeResultTaskResults[DetectEntitiesScoreV3]]],
                           entityRecognitionPii: Option[Seq[TAAnalyzeResultTaskResults[PIIDocV3]]],
                           keyPhraseExtraction: Option[Seq[TAAnalyzeResultTaskResults[KeyPhraseScoreV3]]],
                           sentimentAnalysis: Option[Seq[TAAnalyzeResultTaskResults[SentimentScoredDocumentV3]]])

object TAAnalyzeResults extends SparkBindings[TAAnalyzeResult]
