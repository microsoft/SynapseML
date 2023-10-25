// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.speech

import com.microsoft.azure.synapse.ml.cognitive.{HasServiceParams, HasSetLinkedServiceUsingLocation, HasSetLocation,
  HasSubscriptionKey}
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.io.http.{HasErrorCol, HasURL}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.cognitiveservices.speech._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IOUtils => HUtils}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.SerializableConfiguration
import spray.json.DefaultJsonProtocol._

import java.io.ByteArrayInputStream
import java.net.URI

object TextToSpeech extends ComplexParamsReadable[TextToSpeech] with Serializable

class TextToSpeech(override val uid: String)
  extends Transformer
    with HasSetLocation with HasServiceParams
    with HasErrorCol with HasURL with HasSubscriptionKey with ComplexParamsWritable with SynapseMLLogging
    with HasSetLinkedServiceUsingLocation {
  logClass(FeatureNames.CognitiveServices.Speech)

  setDefault(errorCol -> (uid + "_errors"))

  def this() = this(Identifiable.randomUID("TextToSpeech"))

  def urlPath: String = "/sts/v1.0/issuetoken"

  val possibleOutputFormats: Seq[String] = SpeechSynthesisOutputFormat.values().map(_.toString)

  val outputFormat = new ServiceParam[String](this,
    "outputFormat",
    s"The format for the output audio can be one of $possibleOutputFormats",
    isRequired = false,
    isValid = {
      case Left(f) =>
        assert(possibleOutputFormats.contains(f), s"Must be one of $possibleOutputFormats")
        true
      case Right(_) => true
    })

  def setOutputFormat(v: String): this.type = setScalarParam(outputFormat, v)

  def setOutputFormatCol(v: String): this.type = setVectorParam(outputFormat, v)

  setDefault(outputFormat -> Left("Audio24Khz96KBitRateMonoMp3"))

  val locale = new ServiceParam[String](this,
    "locale",
    s"The locale of the input text",
    isRequired = true)

  def setLocale(v: String): this.type = setScalarParam(locale, v)

  def setLocaleCol(v: String): this.type = setVectorParam(locale, v)

  setDefault(locale -> Left("en-US"))

  val voiceName = new ServiceParam[String](this,
    "voiceName",
    s"The name of the voice used for synthesis",
    isRequired = true)

  def setVoiceName(v: String): this.type = setScalarParam(voiceName, v)

  def setVoiceNameCol(v: String): this.type = setVectorParam(voiceName, v)

  setDefault(voiceName -> Left("en-US-SaraNeural"))

  val language = new ServiceParam[String](this,
    "language",
    s"The name of the language used for synthesis",
    isRequired = true)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  val text = new ServiceParam[String](this,
    "text",
    s"The text to synthesize",
    isRequired = true)

  def setText(v: String): this.type = setScalarParam(text, v)

  def setTextCol(v: String): this.type = setVectorParam(text, v)

  val outputFileCol = new Param[String](this,
    "outputFileCol",
    s"The location of the saved file as an HDFS compliant URI")

  def setOutputFileCol(v: String): this.type = set(outputFileCol, v)

  def getOutputFileCol: String = $(outputFileCol)

  val useSSML = new ServiceParam[Boolean](this,
    "useSSML",
    s"whether to interpret the provided text input as SSML (Speech Synthesis Markup Language). " +
      "The default value is false.",
    isRequired = false)

  def setUseSSML(v: Boolean): this.type = setScalarParam(useSSML, v)

  def setUseSSMLCol(v: String): this.type = setVectorParam(useSSML, v)

  def speechGenerator(synth: SpeechSynthesizer, shouldUseSSML: Boolean, txt: String): SpeechSynthesisResult = {
    if (shouldUseSSML) synth.SpeakSsml(txt) else synth.SpeakText(txt)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val hconf = new SerializableConfiguration(dataset.sparkSession.sparkContext.hadoopConfiguration)
    val toRow = SpeechSynthesisError.makeToRowConverter
    dataset.toDF().map { row =>
      using(SpeechConfig.fromEndpoint(new URI(getUrl), getValue(row, subscriptionKey))) { config =>
        getValueOpt(row, language).foreach(lang => config.setSpeechSynthesisLanguage(lang))
        getValueOpt(row, voiceName).foreach(voice => config.setSpeechSynthesisVoiceName(voice))
        getValueOpt(row, outputFormat).foreach(format =>
          config.setSpeechSynthesisOutputFormat(SpeechSynthesisOutputFormat.valueOf(format)))

        val (errorOpt, data) = using(new SpeechSynthesizer(config, null)) { synth => //scalastyle:ignore null
          val res = speechGenerator(synth, getValueOpt(row, useSSML)
            .getOrElse(false), getValueOpt(row, text).getOrElse(""))
          val error = if (res.getReason.name() == "SynthesizingAudioCompleted") {
            None
          } else {
            Some(SpeechSynthesisCancellationDetails.fromResult(res))
          }
          (error, res.getAudioData)
        }.get

        val errorRow = errorOpt.map(e => toRow(SpeechSynthesisError.fromSDK(e))).orNull
        if (errorOpt.isEmpty) {
          val path = new Path(row.getString(row.fieldIndex(getOutputFileCol)))
          val fs = FileSystem.get(path.toUri, hconf.value)

          using(fs.create(path)) { os =>
            HUtils.copyBytes(new ByteArrayInputStream(data), os, hconf.value)
          }.get
        }
        Row.fromSeq(row.toSeq ++ Seq(errorRow))
      }.get
    }(RowEncoder(dataset.schema.add(getErrorCol, SpeechSynthesisError.schema)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getErrorCol, SpeechSynthesisError.schema)
  }
}
