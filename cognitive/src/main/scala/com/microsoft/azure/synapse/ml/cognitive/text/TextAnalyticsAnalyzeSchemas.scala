// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.text

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings

import scala.collection.JavaConverters._

// Text Analytics /analyze endpoint schemas

case class TextAnalyzeRequest(displayName: String,
                              analysisInput: TextAnalyzeInput,
                              tasks: TextAnalyzeTasks)

object TextAnalyzeRequest extends SparkBindings[TextAnalyzeRequest]

case class TextAnalyzeInput(documents: Seq[TADocument])

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


case class TextAnalyzeAPIResults[T <: HasDocId](state: String,
                                                lastUpdateDateTime: Option[String],
                                                results: Option[TAResponse[T]])

case class TextAnalyzeAPITasks(completed: Int,
                               failed: Int,
                               inProgress: Int,
                               total: Int,
                               entityRecognitionTasks: Option[Seq[TextAnalyzeAPIResults[NERScoredDoc]]],
                               entityLinkingTasks: Option[Seq[TextAnalyzeAPIResults[EntityDetectorScoredDoc]]],
                               entityRecognitionPiiTasks: Option[Seq[TextAnalyzeAPIResults[PIIScoredDoc]]],
                               keyPhraseExtractionTasks: Option[Seq[TextAnalyzeAPIResults[KeyPhraseScoredDoc]]],
                               sentimentAnalysisTasks: Option[Seq[TextAnalyzeAPIResults[TextSentimentScoredDoc]]])

// API call response
case class TextAnalyzeResponse(status: String,
                               errors: Option[Seq[TAError]],
                               displayName: String,
                               tasks: TextAnalyzeAPITasks)

object TextAnalyzeResponse extends SparkBindings[TextAnalyzeResponse]

case class UnpackedTextAnalyzeResponse(entityRecognition: Option[UnpackedTAResponse[NERScoredDoc]],
                                       entityLinking: Option[UnpackedTAResponse[EntityDetectorScoredDoc]],
                                       pii: Option[UnpackedTAResponse[PIIScoredDoc]],
                                       keyPhraseExtraction: Option[UnpackedTAResponse[KeyPhraseScoredDoc]],
                                       sentimentAnalysis: Option[UnpackedTAResponse[TextSentimentScoredDoc]])

object UnpackedTextAnalyzeResponse extends SparkBindings[UnpackedTextAnalyzeResponse]
