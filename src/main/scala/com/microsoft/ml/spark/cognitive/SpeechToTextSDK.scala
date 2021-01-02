// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{BufferedInputStream, ByteArrayInputStream, File, FileOutputStream, InputStream}
import java.lang.ProcessBuilder.Redirect
import java.net.{URI, URL}
import java.util.concurrent.LinkedBlockingQueue

import com.microsoft.cognitiveservices.speech._
import com.microsoft.cognitiveservices.speech.audio._
import com.microsoft.cognitiveservices.speech.internal.AudioStreamContainerFormat
import com.microsoft.cognitiveservices.speech.util.EventHandler
import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.cognitive.SpeechFormat._
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.io.http.HasURL
import com.microsoft.ml.spark.{CompressedStream, WavStream}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.injections.SConf
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json._

import scala.language.existentials

object OsUtils {
  val IsWindows: Boolean = System.getProperty("os.name").toLowerCase().indexOf("win") >= 0
}

object SpeechToTextSDK extends ComplexParamsReadable[SpeechToTextSDK]

private[ml] class BlockingQueueIterator[T](lbq: LinkedBlockingQueue[Option[T]],
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

  val audioDataCol = new Param[String](this, "audioDataCol",
    "Column holding audio data, must be either ByteArrays or Strings representing file URIs"
  )

  def setAudioDataCol(v: String): this.type = set(audioDataCol, v)

  def getAudioDataCol: String = $(audioDataCol)

  val recordAudioData = new BooleanParam(this, "recordAudioData",
    "Whether to record audio data to a file location, for use only with m3u8 streams"
  )

  val endpointId = new Param[String](this, "endpointId",
    "endpoint for custom speech models"
  )

  def setEndpointId(v: String): this.type = set(endpointId, v)

  def getEndpointId: String = $(endpointId)

  def setRecordAudioData(v: Boolean): this.type = set(recordAudioData, v)

  def getRecordAudioData: Boolean = $(recordAudioData)

  setDefault(recordAudioData -> false)

  val recordedFileNameCol = new Param[String](this, "recordedFileNameCol",
    "Column holding file names to write audio data to if ``recordAudioData'' is set to true"
  )

  def setRecordedFileNameCol(v: String): this.type = set(recordedFileNameCol, v)

  def getRecordedFileNameCol: String = $(recordedFileNameCol)

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

  val extraFfmpegArgs = new StringArrayParam(this, "extraFfmpegArgs",
    "extra arguments to for ffmpeg output decoding")

  def setExtraFfmpegArgs(v: Array[String]): this.type = set(extraFfmpegArgs, v)

  def getExtraFfmpegArgs: Array[String] = $(extraFfmpegArgs)

  setDefault(extraFfmpegArgs -> Array())

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

  private[ml] def getAudioFormat(fileType: String, default: Option[String]): AudioStreamFormat = {
    fileType.toLowerCase match {
      case "wav" =>
        AudioStreamFormat.getDefaultInputFormat
      case "mp3" =>
        AudioStreamFormat.getCompressedFormat(AudioStreamContainerFormat.MP3)
      case "ogg" =>
        AudioStreamFormat.getCompressedFormat(AudioStreamContainerFormat.OGG_OPUS)
      case _ if default.isDefined =>
        log.warn(s"Could not identify codec $fileType using default ${default.get} instead")
        getAudioFormat(default.get, None)
      case _ =>
        throw new IllegalArgumentException(s"Could not identify codec $fileType")
    }
  }

  /** @return text transcription of the audio */
  def inputStreamToText(stream: InputStream,
                        audioFormat: String,
                        uri: URI,
                        speechKey: String,
                        profanity: String,
                        language: String,
                        format: String,
                        defaultAudioFormat: Option[String]
                       ): Iterator[SpeechResponse] = {
    val speechConfig: SpeechConfig = SpeechConfig.fromEndpoint(uri, speechKey)
    assert(speechConfig != null)
    get(endpointId).foreach(id => speechConfig.setEndpointId(id))
    speechConfig.setProperty(PropertyId.SpeechServiceResponse_ProfanityOption, profanity)
    speechConfig.setSpeechRecognitionLanguage(language)
    speechConfig.setProperty(PropertyId.SpeechServiceResponse_OutputFormatOption, format)

    val af = getAudioFormat(audioFormat, defaultAudioFormat)
    val pullStream = if (audioFormat == "wav") {
      AudioInputStream.createPullStream(new WavStream(stream), af)
    } else {
      AudioInputStream.createPullStream(new CompressedStream(stream), af)
    }
    val audioConfig = AudioConfig.fromStreamInput(pullStream)
    try {
      val recognizer = new SpeechRecognizer(speechConfig, audioConfig)
      val connection = Connection.fromRecognizer(recognizer)
      connection.setMessageProperty("speech.config", "application",
        s"""{"name":"mmlspark", "version": "${BuildInfo.version}"}""")
      val queue = new LinkedBlockingQueue[Option[String]]()

      def recognizedHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
        println(e)
        if (e.getResult.getReason eq ResultReason.RecognizedSpeech) {
          queue.put(Some(e.getResult.getProperties.getProperty(PropertyId.SpeechServiceResponse_JsonResult)))
        }
      }

      def sessionStoppedHandler(s: Any, e: SessionEventArgs): Unit = {
        queue.put(None)
      }

      recognizer.recognized.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizedHandler))
      recognizer.sessionStopped.addEventListener(makeEventHandler[SessionEventArgs](sessionStoppedHandler))
      recognizer.startContinuousRecognitionAsync.get
      new BlockingQueueIterator[String](queue, {
        recognizer.stopContinuousRecognitionAsync.get()
        pullStream.close()
      }).map { jsonString =>
        println(jsonString)
        jsonString.parseJson.convertTo[SpeechResponse]
      }
    } finally {
      speechConfig.close()
      audioConfig.close()
    }
  }

  private def getStream(bconf: Broadcast[SConf],
                        isUriAudio: Boolean,
                        row: Row,
                        dynamicParamRow: Row): (InputStream, String) = {
    if (isUriAudio) {
      val uri = row.getAs[String](getAudioDataCol)
      val ffmpegCommand: Seq[String] = {
        val body = Seq("ffmpeg", "-y",
          "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2000",
          "-i", uri) ++ getExtraFfmpegArgs ++ Seq("-acodec", "mp3", "-ab", "257k", "-f", "mp3", "pipe:1")

        if (getRecordAudioData && OsUtils.IsWindows) {
          val fn = row.getAs[String](getRecordedFileNameCol)
          body ++ Seq("-acodec", "mp3", "-ab", "257k", "-f", "mp3", fn)
        } else if (getRecordAudioData && !OsUtils.IsWindows) {
          val fn = row.getAs[String](getRecordedFileNameCol)
          Seq("/bin/sh", "-c",  (body ++ Seq("|", "tee", fn)).mkString(" "))
        } else {
          body
        }
      }

      val extension = FilenameUtils.getExtension(new URI(uri).getPath).toLowerCase()

      if (Set("m3u8", "m4a")(extension) && uri.startsWith("http")) {
        val stream = new ProcessBuilder()
          .redirectError(Redirect.INHERIT)
          .redirectInput(Redirect.INHERIT)
          .command(ffmpegCommand: _*)
          .start()
          .getInputStream
        (stream, "mp3")
      } else if (uri.startsWith("http")) {
        val conn = new URL(uri).openConnection
        conn.setConnectTimeout(5000)
        conn.setReadTimeout(5000)
        conn.connect()
        (new BufferedInputStream(conn.getInputStream), extension)
      } else {
        val path = new Path(uri)
        val fs = path.getFileSystem(bconf.value.value2)
        (fs.open(path), extension)
      }
    } else {
      val bytes = row.getAs[Array[Byte]](getAudioDataCol)
      (new ByteArrayInputStream(bytes), getValueOpt(dynamicParamRow, fileType).getOrElse("wav"))
    }
  }

  protected def transformAudioRows(dynamicParamColName: String,
                                   toRow: SpeechResponse => Row,
                                   bconf: Broadcast[SConf],
                                   isUriAudio: Boolean)(rows: Iterator[Row]): Iterator[Row] = {
    rows.flatMap { row =>
      if (shouldSkip(row)) {
        Seq(Row.merge(row, Row(None)))
      } else {
        val dynamicParamRow = row.getAs[Row](dynamicParamColName)
        val (stream, audioFileFormat) = getStream(bconf, isUriAudio, row, dynamicParamRow)
        val results = inputStreamToText(
          stream,
          audioFileFormat,
          new URI(getUrl),
          getValue(dynamicParamRow, subscriptionKey),
          getValue(dynamicParamRow, profanity),
          getValue(dynamicParamRow, language),
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
    val sc = df.sparkSession.sparkContext
    val bConf = sc.broadcast(new SConf(sc.hadoopConfiguration))
    val isUriAudio = df.schema(getAudioDataCol).dataType match {
      case StringType => true
      case BinaryType => false
      case t => throw new IllegalArgumentException(s"AudioDataCol must be String or Binary Type, got: ${t}")
    }
    val toRow = SpeechResponse.makeToRowConverter
    enrichedDf.mapPartitions(transformAudioRows(
      dynamicParamColName,
      toRow,
      bConf,
      isUriAudio
    ))(enc)
      .drop(dynamicParamColName)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema(getAudioDataCol).dataType match {
      case StringType => ()
      case BinaryType => ()
      case t => throw new IllegalArgumentException(s"AudioDataCol must be String or Binary Type, got: ${t}")
    }
    if (getStreamIntermediateResults) {
      schema.add(getOutputCol, SpeechResponse.schema)
    } else {
      schema.add(getOutputCol, ArrayType(SpeechResponse.schema))
    }
  }

}
