// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.FileInputStream
import java.net.URI
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.LinkedBlockingQueue

import com.microsoft.cognitiveservices.speech._
import com.microsoft.cognitiveservices.speech.audio.{
  AudioConfig, AudioInputStream, AudioStreamFormat, PushAudioInputStream
}
import com.microsoft.cognitiveservices.speech.internal.AudioStreamContainerFormat
import com.microsoft.cognitiveservices.speech.util.EventHandler
import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.cognitive.SpeechFormat._
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.io.http.HasURL
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.param.{BooleanParam, ParamMap, ServiceParam, ServiceParamData}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json._

import scala.language.existentials

object SpeechToTextSDK extends ComplexParamsReadable[SpeechToTextSDK]

private[ml] class BlockingQueueIterator[T](
                                            lbq: LinkedBlockingQueue[Option[T]],
                                            onFinish: => Unit) extends Iterator[T] {
  var nextVar: Option[T] = None
  var isDone = false
  var takeAnother = true

  override def hasNext: Boolean = {
    if (takeAnother) {
      nextVar = lbq.take()
      takeAnother = false
      isDone = nextVar.isEmpty
    }
    if (isDone) {
      onFinish
    }
    !isDone
  }

  override def next(): T = {
    takeAnother = true
    nextVar.get
  }
}

class SpeechToTextSDK(override val uid: String) extends Transformer
  with HasSetLocation with HasServiceParams
  with HasOutputCol with HasURL with HasSubscriptionKey with ComplexParamsWritable {

  def this() = this(Identifiable.randomUID("SpeechToTextSDK"))

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

  val fileType = new ServiceParam[String](
    this, "fileType", "The file type of the sound files, supported types: wav, ogg, mp3")

  def setFileType(v: String): this.type = setScalarParam(fileType, v)

  def setFileTypeCol(v: String): this.type = setVectorParam(fileType, v)

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

  val streamIntermediateResults = new BooleanParam(this, "streamIntermediateResults",
    "Whether or not to immediately return itermediate results, or group in a sequence"
  )

  def setStreamIntermediateResults(v: Boolean): this.type = set(streamIntermediateResults, v)

  def getStreamIntermediateResults: Boolean = $(streamIntermediateResults)

  setDefault(streamIntermediateResults -> true)

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

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/sts/v1.0/issuetoken")

  setDefault(language -> ServiceParamData(None, Some("en-us")))
  setDefault(profanity -> ServiceParamData(None, Some("Masked")))
  setDefault(format -> ServiceParamData(None, Some("Simple")))

  def makeEventHandler[T](f: (Any, T) => Unit): EventHandler[T] = {
    new EventHandler[T] {
      def onEvent(var1: Any, var2: T): Unit = f(var1, var2)
    }
  }

  private def getAudioFormat(fileTypeOption: Option[String], bytes: Array[Byte]) = {
    val fileType = fileTypeOption.map(_.toLowerCase()).getOrElse("wav")
    fileType match {
      case "wav" =>
        val bb1 = ByteBuffer.allocate(2)
        bb1.order(ByteOrder.LITTLE_ENDIAN)
        bb1.put(bytes(22))
        bb1.put(bytes(23))
        val nChannels = bb1.getShort(0)

        val bb2 = ByteBuffer.allocate(4)
        bb2.order(ByteOrder.LITTLE_ENDIAN)
        bb2.put(bytes(24))
        bb2.put(bytes(25))
        bb2.put(bytes(26))
        bb2.put(bytes(27))
        val sampleRate = bb2.getInt(0)

        val bb3 = ByteBuffer.allocate(2)
        bb3.order(ByteOrder.LITTLE_ENDIAN)
        bb3.put(bytes(34))
        bb3.put(bytes(35))
        val bitsPerSample = bb3.getShort(0)

        AudioStreamFormat.getWaveFormatPCM(sampleRate, bitsPerSample, nChannels)
      case "mp3" =>
        AudioStreamFormat.getCompressedFormat(AudioStreamContainerFormat.MP3)
      case "ogg" =>
        AudioStreamFormat.getCompressedFormat(AudioStreamContainerFormat.OGG_OPUS)
    }

  }

  /** @return text transcription of the audio */
  def audioBytesToText(bytes: Array[Byte],
                       speechKey: String,
                       uri: URI,
                       language: String,
                       profanity: String,
                       format: String,
                       fileType: Option[String]): Iterator[SpeechResponse] = {
    val config: SpeechConfig = SpeechConfig.fromEndpoint(uri, speechKey)
    assert(config != null)
    config.setProperty(PropertyId.SpeechServiceResponse_ProfanityOption, profanity)
    config.setSpeechRecognitionLanguage(language)
    config.setProperty(PropertyId.SpeechServiceResponse_OutputFormatOption, format)
    val pushStream: PushAudioInputStream = AudioInputStream.createPushStream(getAudioFormat(fileType, bytes))
    val audioInput: AudioConfig = AudioConfig.fromStreamInput(pushStream)
    try {
      val recognizer = new SpeechRecognizer(config, audioInput)
      val connection = Connection.fromRecognizer(recognizer)
      connection.setMessageProperty("speech.config", "application",
        s"""{"name":"mmlspark", "version": "${BuildInfo.version}"}""")
      val results = new LinkedBlockingQueue[Option[String]]()

      def recognizedHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
        if (e.getResult.getReason eq ResultReason.RecognizedSpeech) {
          results.put(Some(e.getResult.getProperties.getProperty(PropertyId.SpeechServiceResponse_JsonResult)))
        }
      }

      def sessionStoppedHandler(s: Any, e: SessionEventArgs): Unit = {
        results.put(None)
      }

      recognizer.recognized.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizedHandler))
      recognizer.sessionStopped.addEventListener(makeEventHandler[SessionEventArgs](sessionStoppedHandler))
      recognizer.startContinuousRecognitionAsync.get
      pushStream.write(bytes)
      pushStream.close()
      new BlockingQueueIterator[String](results, recognizer.stopContinuousRecognitionAsync.get())
        .map(jsonString => jsonString.parseJson.convertTo[SpeechResponse])
    } finally {
      config.close()
      audioInput.close()
      pushStream.close()
    }
  }

  def wavToBytes(filepath: String): Array[Byte] = {
    IOUtils.toByteArray(new FileInputStream(filepath))
  }

  protected def transformAudioRows(dynamicParamColName: String,
                                   toRow: SpeechResponse => Row)(rows: Iterator[Row]): Iterator[Row] = {
    rows.flatMap { row =>
      if (shouldSkip(row)) {
        Seq(Row.merge(row, Row(None)))
      } else {
        val dynamicParamRow = row.getAs[Row](dynamicParamColName)
        val results = audioBytesToText(
          getValue(dynamicParamRow, audioData),
          getValue(dynamicParamRow, subscriptionKey),
          new URI(getUrl),
          getValue(dynamicParamRow, language),
          getValue(dynamicParamRow, profanity),
          getValue(dynamicParamRow, format),
          getValueOpt(dynamicParamRow, fileType))
        if (getStreamIntermediateResults) {
          results.map(speechResponse => Row.merge(row, Row(toRow(speechResponse))))
        } else {
          Seq(Row.merge(row, Row(results.map(speechResponse => toRow(speechResponse)).toSeq)))
        }
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
    val enrichedDf = df.withColumn(dynamicParamColName, struct(dynamicParamCols: _*))
    val addedSchema = if (getStreamIntermediateResults) {
      SpeechResponse.schema
    } else {
      ArrayType(SpeechResponse.schema)
    }

    val enc = RowEncoder(enrichedDf.schema.add(getOutputCol, addedSchema))
    val toRow = SpeechResponse.makeToRowConverter
    enrichedDf.mapPartitions(transformAudioRows(dynamicParamColName, toRow))(enc)
      .drop(dynamicParamColName)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    if (getStreamIntermediateResults) {
      schema.add(getOutputCol, SpeechResponse.schema)
    } else {
      schema.add(getOutputCol, ArrayType(SpeechResponse.schema))
    }
  }

}
