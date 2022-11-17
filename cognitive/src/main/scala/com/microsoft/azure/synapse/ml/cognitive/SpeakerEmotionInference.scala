// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.stages.Lambda
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import spray.json.DefaultJsonProtocol.StringJsonFormat

object SpeakerEmotionInference extends ComplexParamsReadable[SpeakerEmotionInference] with Serializable

class SpeakerEmotionInference(override val uid: String)
  extends CognitiveServicesBase(uid)
    with HasLocaleCol with HasVoiceNameCol with HasTextCol with HasSetLocation
    with HasCognitiveServiceInput with HasInternalJsonOutputParser with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID(classOf[SpeakerEmotionInference].getSimpleName))

  setDefault(
    locale -> Left("en-US"),
    voiceName -> Left("en-US-JennyNeural"),
    text -> Left(this.uid + "_input"),
  )

  def urlPath: String = "cognitiveservices/v1"

  override protected def responseDataType: DataType = SpeakerEmotionInferenceResponse.schema

  protected val additionalHeaders: Map[String, String] = Map[String, String](
    ("X-Microsoft-OutputFormat", "textanalytics-json"),
    ("Content-Type", "application/ssml+xml")
  )

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = super.inputFunc(schema)
    .andThen(r => r.map(r => {
      additionalHeaders.foreach(header => r.setHeader(header._1, header._2))
      r
    }))

  override def setLocation(v: String): this.type = {
    val domain = getLocationDomain(v)
    setUrl(s"https://$v.tts.speech.microsoft.$domain/cognitiveservices/v1")
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { row =>
    val body: String =
      s"<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xmlns:mstts='https://www.w3.org/2001/mstts'" +
        s" xml:lang='en-US'><voice name='Microsoft Server Speech Text to Speech Voice (en-US, JennyNeural)'>" +
        s"<mstts:task name ='RoleStyle'/>${getValue(row, text)}</voice></speak>"
    Some(new StringEntity(body))
  }

  private[cognitive] def formatSSML(content: String,
                                    lang: String,
                                    voice: String,
                                    response: SpeakerEmotionInferenceResponse): String = {
    // Create a sequence containing all of the non-speech text (text outside of quotes)
    // Then zip that with the sequence of all speech text, wrapping speech text in an express-as tag
    val speechBounds = response.Conversations.unzip(c => (c.Begin, c.End))
    val nonSpeechEnds = speechBounds._1 ++ Seq(content.length)
    val nonSpeechBegins = Seq(0) ++ speechBounds._2
    val nonSpeechText = (nonSpeechBegins).zip(nonSpeechEnds).map(pair => content.substring(pair._1, pair._2))
    val speechText = response.Conversations.map(c => {
      s"<mstts:express-as role='${c.Role}' style='${c.Style}'>${c.Content}</mstts:express-as>"
    }) ++ Seq("")
    val innerText = nonSpeechText.zip(speechText).map(pair => pair._1 + pair._2).reduce(_ + _)

    "<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xmlns:mstts='https://www.w3.org/2001/mstts' " +
      s"xml:lang='${lang}'><voice name='${voice}'>${innerText}</voice></speak>\n"
  }

  protected override def getInternalTransformer(schema: StructType): PipelineModel = {
    val internalTransformer = super.getInternalTransformer(schema)
    NamespaceInjections.pipelineModel(stages = Array[Transformer](
      internalTransformer,
      new Lambda().setTransform(ds => {
        val converter = SpeakerEmotionInferenceResponse.makeFromRowConverter
        val newSchema = schema.add(getErrorCol, SpeakerEmotionInferenceError.schema).add(getOutputCol, StringType)
        ds.toDF().map(row => {
          val ssml = formatSSML(
            getValue(row, text),
            getValue(row, locale),
            getValue(row, voiceName),
            converter(row.getAs[Row](row.fieldIndex(getOutputCol)))
          )
          new GenericRowWithSchema((row.toSeq.dropRight(1) ++ Seq(ssml)).toArray, newSchema): Row
        })(RowEncoder({
          newSchema
        }))
      })
    ))
  }
}

trait HasLocaleCol extends HasServiceParams {
  val locale = new ServiceParam[String](this,
    "locale",
    s"The locale of the input text",
    isRequired = true)

  def setLocale(v: String): this.type = setScalarParam(locale, v)

  def setLocaleCol(v: String): this.type = setVectorParam(locale, v)
}

trait HasVoiceNameCol extends HasServiceParams {
  val voiceName = new ServiceParam[String](this,
    "voiceName",
    s"The name of the voice used for synthesis",
    isRequired = true)

  def setVoiceName(v: String): this.type = setScalarParam(voiceName, v)

  def setVoiceNameCol(v: String): this.type = setVectorParam(voiceName, v)
}

trait HasTextCol extends HasServiceParams {
  val text = new ServiceParam[String](this,
    "input",
    s"The text input to annotate with inferred emotion",
    isRequired = true)

  def setText(v: String): this.type = setScalarParam(text, v)

  def setTextCol(v: String): this.type = setVectorParam(text, v)
}
