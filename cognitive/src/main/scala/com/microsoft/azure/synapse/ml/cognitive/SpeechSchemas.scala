// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class DetailedSpeechResponse(Confidence: Double,
                                  Lexical: String,
                                  ITN: String,
                                  MaskedITN: String,
                                  Display: String)

trait SharedSpeechFields {
  def RecognitionStatus: String

  def Offset: Long

  def Duration: Long

  def DisplayText: Option[String]

  def NBest: Option[Seq[DetailedSpeechResponse]]
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
  implicit val DetailedSpeechResponseFormat: RootJsonFormat[DetailedSpeechResponse] =
    jsonFormat5(DetailedSpeechResponse.apply)
  implicit val SpeechResponseFormat: RootJsonFormat[SpeechResponse] = jsonFormat6(SpeechResponse.apply)
  implicit val TranscriptionResponseFormat: RootJsonFormat[TranscriptionResponse] =
    jsonFormat9(TranscriptionResponse.apply)
  implicit val TranscriptionParticipantFormat: RootJsonFormat[TranscriptionParticipant] =
    jsonFormat3(TranscriptionParticipant.apply)

}
