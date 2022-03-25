// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import scala.collection.JavaConverters._

// Text Analytics /analyze endpoint schemas

case class TextAnalyzeInput(documents: Seq[TADocument])

object TextAnalyzeInput extends SparkBindings[TextAnalyzeInput]

case class TextAnalyzeTask(parameters: Map[String, String]) {
  def this(parameters: java.util.HashMap[String, String]) {
    this(parameters.asScala.toMap)
  }
}

object TextAnalyzeTask extends SparkBindings[TextAnalyzeTask]

case class TextAnalyzeTasks(entityRecognitionTasks: Seq[TextAnalyzeTask],
                            entityLinkingTasks: Seq[TextAnalyzeTask],
                            entityRecognitionPiiTasks: Seq[TextAnalyzeTask],
                            keyPhraseExtractionTasks: Seq[TextAnalyzeTask],
                            sentimentAnalysisTasks: Seq[TextAnalyzeTask])

object TextAnalyzeTasks extends SparkBindings[TextAnalyzeTasks]

case class TextAnalyzeRequest(displayName: String,
                              analysisInput: TextAnalyzeInput,
                              tasks: TextAnalyzeTasks)

object TextAnalyzeRequest extends SparkBindings[TextAnalyzeRequest]


case class TextAnalyzeAPIResult[T <: HasDocId](documents: Seq[T],
                                               errors: Seq[TAError],
                                               modelVersion: String)

case class TextAnalyzeAPIResults[T <: HasDocId](state: String,
                                                lastUpdateDateTime: Option[String],
                                                results: Option[TextAnalyzeAPIResult[T]])

case class TextAnalyzeAPITasks(completed: Int,
                               failed: Int,
                               inProgress: Int,
                               total: Int,
                               entityRecognitionTasks: Option[Seq[TextAnalyzeAPIResults[NERDocV3]]],
                               entityLinkingTasks: Option[Seq[TextAnalyzeAPIResults[DetectEntitiesScoreV3]]],
                               entityRecognitionPiiTasks: Option[Seq[TextAnalyzeAPIResults[PIIDocV3]]],
                               keyPhraseExtractionTasks: Option[Seq[TextAnalyzeAPIResults[KeyPhraseScoreV3]]],
                               sentimentAnalysisTasks: Option[Seq[TextAnalyzeAPIResults[SentimentScoredDocumentV3]]])

// API call response
case class TextAnalyzeResponse(status: String,
                               errors: Option[Seq[TAError]],
                               displayName: String,
                               tasks: TextAnalyzeAPITasks)

object TextAnalyzeResponse extends SparkBindings[TextAnalyzeResponse]

case class TextAnalyzeResult[T <: HasDocId](result: Option[T],
                                            error: Option[TAError])

case class TextAnalyzeSimplifiedResponse(entityRecognition: Option[Seq[TextAnalyzeResult[NERDocV3]]],
                                         entityLinking: Option[Seq[TextAnalyzeResult[DetectEntitiesScoreV3]]],
                                         entityRecognitionPii: Option[Seq[TextAnalyzeResult[PIIDocV3]]],
                                         keyPhraseExtraction: Option[Seq[TextAnalyzeResult[KeyPhraseScoreV3]]],
                                         sentimentAnalysis: Option[Seq[TextAnalyzeResult[SentimentScoredDocumentV3]]])

object TextAnalyzeSimplifiedResponse extends SparkBindings[TextAnalyzeSimplifiedResponse]
