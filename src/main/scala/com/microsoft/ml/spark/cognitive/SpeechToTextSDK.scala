// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, InputStream}
import java.net.URI
import java.util
import java.util.Collections

import com.microsoft.cognitiveservices.speech.audio.{AudioConfig, AudioInputStream, PushAudioInputStream}
import com.microsoft.cognitiveservices.speech.util.EventHandler
import com.microsoft.cognitiveservices.speech.{OutputFormat, ProfanityOption, ResultReason, SessionEventArgs, SpeechConfig, SpeechRecognitionEventArgs, SpeechRecognizer}
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import org.apache.commons.compress.utils.IOUtils
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, ServiceParam}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.language.existentials
import scala.util.Try

class SpeechToTextSDK(override val uid: String) extends Transformer
  with HasSetLocation with HasServiceParams with HasOutputCol with HasCognitiveServiceInput {

  def this() = this(Identifiable.randomUID("SpeechToText"))

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

  val region = "eastus" //temp
  val location =
    new URI(s"https://$region.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1")

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1")

  def makeEventHandler[T](f: (Any, T) => Unit): EventHandler[T] = {
    new EventHandler[T] {
      def onEvent(var1: Any, var2: T): Unit = f(var1, var2)
    }
  }

  /**
    * @return text transcription of the audio
    */
  def audioBytesToText(bytes: Array[Byte],
                       speechKey: String,
                       uri: URI,
                       language: String,
                       profanity: ProfanityOption,
                       format: OutputFormat): String = {

    val config: SpeechConfig = SpeechConfig.fromEndpoint(uri, speechKey)
    assert(config != null)
    config.setProfanity(profanity)
    config.setSpeechRecognitionLanguage(language)
    config.setOutputFormat(format)

    val inputStream: InputStream = new ByteArrayInputStream(bytes)
    val pushStream: PushAudioInputStream = AudioInputStream.createPushStream
    val audioInput: AudioConfig = AudioConfig.fromStreamInput(pushStream)

    val recognizer = new SpeechRecognizer(config, audioInput)
    val resultPromise = Promise[String]()
    val stringBuffer = Collections.synchronizedList(new util.ArrayList[String])

    def recognizedHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
      if (e.getResult.getReason eq ResultReason.RecognizedSpeech) {
        stringBuffer.add(e.getResult.getText)
      }
    }

    def sessionStoppedHandler(s: Any, e: SessionEventArgs): Unit = {
      resultPromise.complete(Try(stringBuffer.toArray.mkString(" ")))
    }
    recognizer.recognized.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizedHandler))
    recognizer.sessionStopped.addEventListener(makeEventHandler[SessionEventArgs](sessionStoppedHandler))

    recognizer.startContinuousRecognitionAsync.get
    pushStream.write(bytes)

    pushStream.close()
    inputStream.close()

    val result: String = Await.result(resultPromise.future, Duration.Inf)

    recognizer.stopContinuousRecognitionAsync.get()
    config.close()
    audioInput.close()
    result
  }

  /**
    * @param filename a WAV file in the resources directory
    * @return text transcription of the WAV file
    */
  def wavToText(filename: String,
                speechKey: String,
                uri: URI,
                language: String,
                profanity: ProfanityOption,
                format: OutputFormat): String = {

    val audioBytes = wavToBytes(filename)
    audioBytesToText(audioBytes, speechKey, uri, language, profanity, format)
  }

  def wavToBytes(filepath: String): Array[Byte] = {
    IOUtils.toByteArray(new FileInputStream(filepath))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val speechKey = sys.env.getOrElse("SPEECH_API_KEY", "")
    val uri = location
    val language = "en-us"
    val profanity = ProfanityOption.Masked
    val format = OutputFormat.Simple

    val audioBytesToTextUDF = udf((audioBytes: Array[Byte]) =>
      audioBytesToText(audioBytes, speechKey,  uri, language, profanity, format))
    val df = dataset.toDF()
    df.withColumn(getOutputCol,  audioBytesToTextUDF(df("audio")))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add("text", StringType)
  }

  def convertToWav(data: Array[Byte]): Array[Byte] = { // open stream
    import javax.sound.sampled.AudioFileFormat.Type
    import javax.sound.sampled.{AudioFormat, AudioSystem}

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
      val converted = AudioSystem.getAudioInputStream(format, sourceStream)
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
  }

}
