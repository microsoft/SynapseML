// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import java.net.URI
import java.util
import java.util.Collections

import com.microsoft.cognitiveservices.speech.{OutputFormat, ProfanityOption, ResultReason, SessionEventArgs, SpeechConfig, SpeechRecognitionEventArgs, SpeechRecognizer}
import com.microsoft.cognitiveservices.speech.audio.{AudioConfig, AudioInputStream, PushAudioInputStream}
import com.microsoft.cognitiveservices.speech.util.EventHandler
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.io.http.HasURL
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, ServiceParam, ServiceParamData}
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
  with HasSetLocation with HasServiceParams
  with HasOutputCol with HasURL with HasSubscriptionKey {

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
    new URI(s"https://$region.api.cognitive.microsoft.com/sts/v1.0/issuetoken")

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/sts/v1.0/issuetoken")


  val defaultLanguage = "en-us"
  val defaultProfanity =  "Masked"
  val defaultFormat = "Simple"

  setDefault(language->ServiceParamData(None, Some(defaultLanguage.toString)))
  setDefault(profanity->ServiceParamData(None, Some(defaultProfanity.toString)))
  setDefault(format->ServiceParamData(None, Some(defaultFormat.toString)))

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
                       profanity: String,
                       format: String): String = {
    val config: SpeechConfig = SpeechConfig.fromEndpoint(uri, speechKey)
    assert(config != null)
    config.setProperty("profanity", profanity)
    config.setSpeechRecognitionLanguage(language)

    val inputStream: InputStream = new ByteArrayInputStream(bytes)
    val pushStream: PushAudioInputStream = AudioInputStream.createPushStream
    val audioInput: AudioConfig = AudioConfig.fromStreamInput(pushStream)

    val recognizer = new SpeechRecognizer(config, audioInput)
    val resultPromise = Promise[String]()
    val stringBuffer = Collections.synchronizedList(new util.ArrayList[String])

    def recognizedHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
      if (e.getResult.getReason eq ResultReason.RecognizedSpeech) {
        val text = e.getResult.getText
        stringBuffer.add(e.getResult.getText)
      }
    }

    def recognizingHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
      println("RECOGNIZING: " + e.getResult.getText)
    }

    def sessionStoppedHandler(s: Any, e: SessionEventArgs): Unit = {
      println("STOPPED")
      resultPromise.complete(Try(stringBuffer.toArray.mkString(" ")))
    }

    recognizer.recognizing.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizingHandler))
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

  def wavToBytes(filepath: String): Array[Byte] = {
    IOUtils.toByteArray(new FileInputStream(filepath))
  }

  protected def inputFunc(schema: StructType): Row => Option[String] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        Some(audioBytesToText(
          getValue(row, audioData),
          getValue(row, subscriptionKey),
          new URI(getUrl),
          getValue(row, language),
          getValue(row, profanity),
          getValue(row, format)))
      }
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF
    val schema = dataset.schema
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", dataset)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)

    assert(badColumns.isEmpty,
      s"Could not find dynamic columns: $badColumns in columns: ${schema.fieldNames.toSet}")

    val dynamicParamCols = getVectorParamMap.values.toList.map(col) match {
      case Nil => Seq(lit(false).alias("placeholder"))
      case l => l
    }
    println("Done transforming...")
     df.withColumn(dynamicParamColName, struct(dynamicParamCols: _*))
      .withColumn(getOutputCol, udf(inputFunc(schema), StringType)(col(dynamicParamColName)))
      .drop(dynamicParamColName)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add("text", StringType)
  }

}
