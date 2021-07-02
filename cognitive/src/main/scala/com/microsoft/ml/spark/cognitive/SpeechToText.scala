// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._

import java.io._
import javax.sound.sampled.AudioFileFormat.Type
import javax.sound.sampled._
import scala.language.existentials

object SpeechToText extends ComplexParamsReadable[SpeechToText] with Serializable

class SpeechToText(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation with BasicLogging
  with HasSetLinkedService {
  logClass()

  def this() = this(Identifiable.randomUID("SpeechToText"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1")

  def urlPath: String = "/speech/recognition/conversation/cognitiveservices/v1"

  override def responseDataType: DataType = SpeechResponse.schema

  val audioData = new ServiceParam[Array[Byte]](this, "audioData",
    """
      |The data sent to the service must be a .wav files
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = true,
    isURLParam = false
  )

  def setAudioData(v: Array[Byte]): this.type = setScalarParam(audioData, v)

  def setAudioDataCol(v: String): this.type = setVectorParam(audioData, v)

  val language = new ServiceParam[String](this, "language",
    """
      |Identifies the spoken language that is being recognized.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = true,
    isURLParam = true
  )

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  val format = new ServiceParam[String](this, "format",
    """
      |Specifies the result format. Accepted values are simple and detailed. Default is simple.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = false,
    isURLParam = true
  )

  def setFormat(v: String): this.type = setScalarParam(format, v)

  def setFormatCol(v: String): this.type = setVectorParam(format, v)

  val profanity = new ServiceParam[String](this, "profanity",
    """
      |Specifies how to handle profanity in recognition results.
      |Accepted values are masked, which replaces profanity with asterisks,
      |removed, which remove all profanity from the result, or raw,
      |which includes the profanity in the result. The default setting is masked.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = false,
    isURLParam = true
  )

  def setProfanity(v: String): this.type = setScalarParam(profanity, v)

  def setProfanityCol(v: String): this.type = setVectorParam(profanity, v)

  override protected def contentType: Row => String = { _ => "audio/wav; codec=audio/pcm; samplerate=16000" }

  def convertToWav(data: Array[Byte]): Array[Byte] = { // open stream
    try{
      val sourceStream = AudioSystem.getAudioInputStream(new ByteArrayInputStream(data))
      val sourceFormat: AudioFormat = sourceStream.getFormat
      // create audio format object for the desired stream/audio format
      // this is *not* the same as the file format (wav)
      val format = new AudioFormat(
        AudioFormat.Encoding.PCM_SIGNED,
        sourceFormat.getSampleRate,
        sourceFormat.getSampleSizeInBits,
        sourceFormat.getChannels,
        sourceFormat.getFrameSize,
        sourceFormat.getFrameRate,
        sourceFormat.isBigEndian)

      // create stream that delivers the desired format
      val converted: AudioInputStream = AudioSystem.getAudioInputStream(format, sourceStream)
      // write stream into a file with file format wav
      val os = new ByteArrayOutputStream()
      try {
        AudioSystem.write(converted, Type.WAVE, os)
        os.toByteArray
      } finally {
        os.close()
      }
    } catch {
      //TODO figure out why build machines don't have proper codecs
      case e: javax.sound.sampled.UnsupportedAudioFileException =>
        logWarning(e.getMessage)
        data
    }
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { row =>
    Some(new ByteArrayEntity(convertToWav(getValue(row, audioData))))
    //Some(new ByteArrayEntity(getValue(row,audioData)))
  }

}
