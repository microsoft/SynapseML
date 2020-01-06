//// Copyright (C) Microsoft Corporation. All rights reserved.
//// Licensed under the MIT License. See LICENSE in project root for information.
//
//package com.microsoft.ml.spark.cognitive
//
//import java.io._
//
//import javax.sound.sampled.AudioFileFormat.Type
//import javax.sound.sampled._
//import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity}
//import org.apache.spark.ml.Transformer
//import org.apache.spark.ml.param.{ParamMap, ServiceParam}
//import org.apache.spark.ml.util._
//import org.apache.spark.sql.{DataFrame, Dataset, Row}
//import org.apache.spark.sql.types._
//import spray.json.DefaultJsonProtocol._
//
//import scala.language.existentials
//
//
//class SpeechToTextSDK(override val uid: String) extends Transformer
//  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation {
//
//  def this() = this(Identifiable.randomUID("SpeechToText"))
//
//  def setLocation(v: String): this.type =
//    setUrl(s"https://$v.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1")
//
//  override def responseDataType: DataType = SpeechResponse.schema
//
//  val audioData = new ServiceParam[Array[Byte]](this, "audioData",
//    """
//      |The data sent to the service must be a .wav files
//    """.stripMargin.replace("\n", " ").replace("\r", " "),
//    { _ => true },
//    isRequired = true,
//    isURLParam = false
//  )
//
//  def setAudioData(v: Array[Byte]): this.type = setScalarParam(audioData, v)
//
//  def setAudioDataCol(v: String): this.type = setVectorParam(audioData, v)
//
//  val language = new ServiceParam[String](this, "language",
//    """
//      |Identifies the spoken language that is being recognized.
//    """.stripMargin.replace("\n", " ").replace("\r", " "),
//    { _ => true },
//    isRequired = true,
//    isURLParam = true
//  )
//
//  def setLanguage(v: String): this.type = setScalarParam(language, v)
//
//  def setLanguageCol(v: String): this.type = setVectorParam(language, v)
//
//  val format = new ServiceParam[String](this, "format",
//    """
//      |Specifies the result format. Accepted values are simple and detailed. Default is simple.
//    """.stripMargin.replace("\n", " ").replace("\r", " "),
//    { _ => true },
//    isRequired = false,
//    isURLParam = true
//  )
//
//  def setFormat(v: String): this.type = setScalarParam(format, v)
//
//  def setFormatCol(v: String): this.type = setVectorParam(format, v)
//
//  val profanity = new ServiceParam[String](this, "profanity",
//    """
//      |Specifies how to handle profanity in recognition results.
//      |Accepted values are masked, which replaces profanity with asterisks,
//      |removed, which remove all profanity from the result, or raw,
//      |which includes the profanity in the result. The default setting is masked.
//    """.stripMargin.replace("\n", " ").replace("\r", " "),
//    { _ => true },
//    isRequired = false,
//    isURLParam = true
//  )
//
//  def setProfanity(v: String): this.type = setScalarParam(profanity, v)
//
//  def setProfanityCol(v: String): this.type = setVectorParam(profanity, v)
//
//  override def transform(dataset: Dataset[_]): DataFrame = {
//    val df = dataset.toDF()
//
//}
//
//  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
//
//  override def transformSchema(schema: StructType): StructType = {
//
//  }
//}
