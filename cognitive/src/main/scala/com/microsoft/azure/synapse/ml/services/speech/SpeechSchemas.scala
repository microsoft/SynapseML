// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.speech

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import com.microsoft.cognitiveservices.speech.SpeechSynthesisCancellationDetails
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class Word(Duration: Long, Offset: Long, Word: String)

case class DetailedSpeechResponse(Confidence: Double,
                                  Lexical: String,
                                  ITN: String,
                                  MaskedITN: String,
                                  Words: Option[Seq[Word]],
                                  Display: String)

trait SharedSpeechFields {
  //scalastyle:off method.name
  def RecognitionStatus: String

  def Offset: Long

  def Duration: Long

  def DisplayText: Option[String]

  def NBest: Option[Seq[DetailedSpeechResponse]]
  //scalastyle:on method.name
}

case class SpeechResponse(RecognitionStatus: String,
                          Offset: Long,
                          Duration: Long,
                          Id: Option[String],
                          DisplayText: Option[String],
                          NBest: Option[Seq[DetailedSpeechResponse]]
                         ) extends SharedSpeechFields

object SpeechResponse extends SparkBindings[SpeechResponse]

case class TranscriptionResponse(RecognitionStatus: String,
                                 Offset: Long,
                                 Duration: Long,
                                 Id: Option[String],
                                 DisplayText: Option[String],
                                 NBest: Option[Seq[DetailedSpeechResponse]],
                                 SpeakerId: String,
                                 Type: String,
                                 UtteranceId: String) extends SharedSpeechFields

object TranscriptionResponse extends SparkBindings[TranscriptionResponse]

case class TranscriptionParticipant(name: String, language: String, signature: String)

object SpeechFormat extends DefaultJsonProtocol {
  implicit val WordFormat: RootJsonFormat[Word] =
    jsonFormat3(Word.apply)
  implicit val DetailedSpeechResponseFormat: RootJsonFormat[DetailedSpeechResponse] =
    jsonFormat6(DetailedSpeechResponse.apply)
  implicit val SpeechResponseFormat: RootJsonFormat[SpeechResponse] = jsonFormat6(SpeechResponse.apply)
  implicit val TranscriptionResponseFormat: RootJsonFormat[TranscriptionResponse] =
    jsonFormat9(TranscriptionResponse.apply)
  implicit val TranscriptionParticipantFormat: RootJsonFormat[TranscriptionParticipant] =
    jsonFormat3(TranscriptionParticipant.apply)
}

object SpeechSynthesisError extends SparkBindings[SpeechSynthesisError] {
  def fromSDK(error: SpeechSynthesisCancellationDetails): SpeechSynthesisError = {
    SpeechSynthesisError(error.getErrorCode.name(), error.getErrorDetails, error.getReason.name())
  }
}

case class SpeechSynthesisError(errorCode: String, errorDetails: String, errorReason: String)

object SpeakerEmotionInferenceError extends SparkBindings[SpeakerEmotionInferenceError]

case class SpeakerEmotionInferenceError(errorCode: String, errorDetails: String)

case class SSMLConversation(Begin: Int,
                            End: Int,
                            Content: String,
                            Role: String,
                            Style: String)

object SSMLConversation extends SparkBindings[SSMLConversation]

case class SpeakerEmotionInferenceResponse(IsValid: Boolean, Conversations: Seq[SSMLConversation])

object SpeakerEmotionInferenceResponse extends SparkBindings[SpeakerEmotionInferenceResponse]
