// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http.{HasHandler, SimpleHTTPTransformer}
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.stages.{DropColumns, Lambda, UDFTransformer}
import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.URI
import java.util
import scala.collection.JavaConverters._


trait TextAnalyticsInputParams extends HasServiceParams {
  val text = new ServiceParam[Seq[String]](this, "text", "the text in the request body", isRequired = true)

  def setTextCol(v: String): this.type = setVectorParam(text, v)

  def setText(v: Seq[String]): this.type = setScalarParam(text, v)

  def setText(v: String): this.type = setScalarParam(text, Seq(v))

  setDefault(text -> Right("text"))

  val language = new ServiceParam[Seq[String]](this, "language",
    "the language code of the text (optional for some services)")

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def setLanguage(v: Seq[String]): this.type = setScalarParam(language, v)

  def setLanguage(v: String): this.type = setScalarParam(language, Seq(v))

  //setDefault(language -> Left(Seq("en")))

}

abstract class TextAnalyticsBase(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation
  with HasSetLinkedService with TextAnalyticsInputParams {

  protected def innerResponseDataType: StructType =
    responseDataType("documents").dataType match {
      case ArrayType(idt: StructType, _) => idt
      case _ =>
        throw new IllegalArgumentException("response data types should have a inner type")
    }

  override protected def responseDataType: StructType = {
    new StructType()
      .add("documents", ArrayType(innerResponseDataType))
      .add("errors", ArrayType(TAError.schema))
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else if (getValue(row, text).forall(Option(_).isEmpty)) {
        None
      } else {
        import TAJSONFormat._
        val post = new HttpPost(getUrl)
        getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
        post.setHeader("Content-Type", "application/json")
        val texts = getValue(row, text)

        val languages: Option[Seq[String]] = (getValueOpt(row, language) match {
          case Some(Seq(lang)) => Some(Seq.fill(texts.size)(lang))
          case s => s
        })

        val documents = texts.zipWithIndex.map { case (t, i) =>
          TADocument(languages.flatMap(ls => Option(ls(i))), i.toString, Option(t).getOrElse(""))
        }
        val json = TARequest(documents).toJson.compactPrint
        post.setEntity(new StringEntity(json, "UTF-8"))
        Some(post)
      }
    }
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }

  protected def reshapeToArray(schema: StructType, parameterName: String): Option[(Transformer, String, String)] = {
    val reshapedColName = DatasetExtensions.findUnusedColumnName(parameterName, schema)
    getVectorParamMap.get(parameterName).flatMap {
      case c if schema(c).dataType == StringType =>
        Some((Lambda(_.withColumn(reshapedColName, array(col(getVectorParam(parameterName))))),
          getVectorParam(parameterName),
          reshapedColName))
      case _ => None
    }
  }

  protected def unpackBatchUDF: UserDefinedFunction = {
    val innerFields = innerResponseDataType.fields.filter(_.name != "id")
    UDFUtils.oldUdf({ rowOpt: Row =>
      Option(rowOpt).map { row =>
        val documents = row.getSeq[Row](1).map(doc =>
          (doc.getString(0).toInt, doc)).toMap
        val errors = row.getSeq[Row](2).map(err => (err.getString(0).toInt, err)).toMap
        val rows: Seq[Row] = (0 until (documents.size + errors.size)).map(i =>
          documents.get(i)
            .map(doc => Row.fromSeq(doc.toSeq.tail ++ Seq(None)))
            .getOrElse(Row.fromSeq(
              Seq.fill(innerFields.length)(None) ++ Seq(errors.get(i).map(_.getString(1)).orNull)))
        )
        rows
      }
    }, ArrayType(
      innerResponseDataType.fields.filter(_.name != "id").foldLeft(new StructType()) { case (st, f) =>
        st.add(f.name, f.dataType)
      }.add("error-message", StringType)
    )
    )
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)
    assert(badColumns.isEmpty,
      s"Could not find dynamic input columns: $badColumns in columns: ${schema.fieldNames.toSet}")

    val missingRequiredParams = this.getRequiredParams.filter {
      p => this.get(p).isEmpty && this.getDefault(p).isEmpty
    }
    assert(missingRequiredParams.isEmpty,
      s"Missing required params: ${missingRequiredParams.map(s => s.name).mkString("(", ", ", ")")}")

    val reshapeCols = Seq(
      reshapeToArray(schema, "text"),
      reshapeToArray(schema, "language")).flatten

    val newColumnMapping = reshapeCols.map {
      case (_, oldCol, newCol) => (oldCol, newCol)
    }.toMap

    val columnsToGroup = getVectorParamMap.map { case (_, oldCol) =>
      val newCol = newColumnMapping.getOrElse(oldCol, oldCol)
      col(newCol).alias(oldCol)
    }.toSeq

    val stages = reshapeCols.map(_._1).toArray ++ Array(
      Lambda(_.withColumn(
        dynamicParamColName,
        struct(columnsToGroup: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamColName)
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(get(concurrentTimeout))
        .setErrorCol(getErrorCol),
      new UDFTransformer()
        .setInputCol(getOutputCol)
        .setOutputCol(getOutputCol)
        .setUDF(unpackBatchUDF),
      new DropColumns().setCols(Array(
        dynamicParamColName) ++ newColumnMapping.values.toArray.asInstanceOf[Array[String]])
    )

    NamespaceInjections.pipelineModel(stages)
  }

}

trait HasLanguage extends HasServiceParams {
  val language = new ServiceParam[String](this, "language", "the language to use", isURLParam = true)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def setLanguage(v: String): this.type = setScalarParam(language, v)
}

trait HasModelVersion extends HasServiceParams {
  val modelVersion = new ServiceParam[String](this, "modelVersion",
    "This value indicates which model will be used for scoring." +
      " If a model-version is not specified, the API should default to the latest," +
      " non-preview version.", isURLParam = true)

  def setModelVersion(v: String): this.type = setScalarParam(modelVersion, v)
}

trait HasShowStats extends HasServiceParams {
  val showStats = new ServiceParam[Boolean](this, "showStats",
    "if set to true, response will contain input and document level statistics.", isURLParam = true)

  def setShowStats(v: Boolean): this.type = setScalarParam(showStats, v)
}

trait HasStringIndexType extends HasServiceParams {
  val stringIndexType = new ServiceParam[String](this, "stringIndexType",
    "Specifies the method used to interpret string offsets. " +
      "Defaults to Text Elements (Graphemes) according to Unicode v8.0.0. " +
      "For additional information see https://aka.ms/text-analytics-offsets", isURLParam = true)

  def setStringIndexType(v: String): this.type = setScalarParam(stringIndexType, v)
}

object TextSentimentV2 extends ComplexParamsReadable[TextSentimentV2]

class TextSentimentV2(override val uid: String)
  extends TextAnalyticsBase(uid) with BasicLogging with HasHandler {
  logClass()

  def this() = this(Identifiable.randomUID("TextSentimentV2"))

  override def responseDataType: StructType = SentimentResponseV2.schema

  def urlPath: String = "/text/analytics/v2.0/sentiment"

}

object LanguageDetectorV2 extends ComplexParamsReadable[LanguageDetectorV2]

class LanguageDetectorV2(override val uid: String)
  extends TextAnalyticsBase(uid) with BasicLogging with HasHandler {
  logClass()

  def this() = this(Identifiable.randomUID("LanguageDetectorV2"))

  override def responseDataType: StructType = DetectLanguageResponseV2.schema

  def urlPath: String = "/text/analytics/v2.0/languages"
}

object EntityDetectorV2 extends ComplexParamsReadable[EntityDetectorV2]

class EntityDetectorV2(override val uid: String)
  extends TextAnalyticsBase(uid) with BasicLogging with HasHandler {
  logClass()

  def this() = this(Identifiable.randomUID("EntityDetectorV2"))

  override def responseDataType: StructType = DetectEntitiesResponseV2.schema

  def urlPath: String = "/text/analytics/v2.0/entities"
}

object NERV2 extends ComplexParamsReadable[NERV2]

class NERV2(override val uid: String) extends TextAnalyticsBase(uid) with BasicLogging with HasHandler {
  logClass()

  def this() = this(Identifiable.randomUID("NERV2"))

  override def responseDataType: StructType = NERResponseV2.schema

  def urlPath: String = "/text/analytics/v2.1/entities"
}

object KeyPhraseExtractorV2 extends ComplexParamsReadable[KeyPhraseExtractorV2]

class KeyPhraseExtractorV2(override val uid: String)
  extends TextAnalyticsBase(uid) with BasicLogging with HasHandler {
  logClass()

  def this() = this(Identifiable.randomUID("KeyPhraseExtractorV2"))

  override def responseDataType: StructType = KeyPhraseResponseV2.schema

  def urlPath: String = "/text/analytics/v2.0/keyPhrases"
}

trait TAV3Mixins extends HasModelVersion with HasShowStats with HasStringIndexType with BasicLogging with HasHandler

object TextSentiment extends ComplexParamsReadable[TextSentiment]

class TextSentiment(override val uid: String)
  extends TextAnalyticsBase(uid) with TAV3Mixins {
  logClass()

  def this() = this(Identifiable.randomUID("TextSentiment"))

  val opinionMining = new ServiceParam[Boolean](this, "opinionMining",
    "if set to true, response will contain not only sentiment prediction but also opinion mining " +
      "(aspect-based sentiment analysis) results.", isURLParam = true)

  def setOpinionMining(v: Boolean): this.type = setScalarParam(opinionMining, v)

  override def responseDataType: StructType = SentimentResponseV3.schema

  def urlPath: String = "/text/analytics/v3.1/sentiment"

  override def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = { r: Row =>
    super.inputFunc(schema)(r).map { request =>
      request.setURI(new URI(prepareUrl(r)))
      request
    }
  }
}

object KeyPhraseExtractor extends ComplexParamsReadable[KeyPhraseExtractor]

class KeyPhraseExtractor(override val uid: String)
  extends TextAnalyticsBase(uid) with TAV3Mixins {
  logClass()

  def this() = this(Identifiable.randomUID("KeyPhraseExtractor"))

  override def responseDataType: StructType = KeyPhraseResponseV3.schema

  def urlPath: String = "/text/analytics/v3.1/keyPhrases"
}

object NER extends ComplexParamsReadable[NER]

class NER(override val uid: String)
  extends TextAnalyticsBase(uid) with HasModelVersion
    with HasShowStats with HasStringIndexType with BasicLogging with HasHandler {
  logClass()

  def this() = this(Identifiable.randomUID("NER"))

  override def responseDataType: StructType = NERResponseV3.schema

  def urlPath: String = "/text/analytics/v3.1/entities/recognition/general"
}

object PII extends ComplexParamsReadable[PII]

class PII(override val uid: String)
  extends TextAnalyticsBase(uid) with TAV3Mixins {
  logClass()

  def this() = this(Identifiable.randomUID("PII"))

  val domain = new ServiceParam[String](this, "domain",
    "if specified, will set the PII domain to include only a subset of the entity categories. " +
      "Possible values include: 'PHI', 'none'.", isURLParam = true)
  val piiCategories = new ServiceParam[Seq[String]](this, "piiCategories",
    "describes the PII categories to return", isURLParam = true)

  def setDomain(v: String): this.type = setScalarParam(domain, v)

  def setPiiCategories(v: Seq[String]): this.type = setScalarParam(piiCategories, v)

  override def responseDataType: StructType = PIIResponseV3.schema

  def urlPath: String = "/text/analytics/v3.1/entities/recognition/pii"
}

object LanguageDetector extends ComplexParamsReadable[LanguageDetector]

class LanguageDetector(override val uid: String)
  extends TextAnalyticsBase(uid) with HasModelVersion with HasShowStats with BasicLogging with HasHandler {
  logClass()

  def this() = this(Identifiable.randomUID("LanguageDetector"))

  override def responseDataType: StructType = DetectLanguageResponseV3.schema

  def urlPath: String = "/text/analytics/v3.1/languages"
}

object EntityDetector extends ComplexParamsReadable[EntityDetector]

class EntityDetector(override val uid: String)
  extends TextAnalyticsBase(uid) with TAV3Mixins {
  logClass()

  def this() = this(Identifiable.randomUID("EntityDetector"))

  override def responseDataType: StructType = DetectEntitiesResponseV3.schema

  def urlPath: String = "/text/analytics/v3.1/entities/linking"
}


class TextAnalyzeTaskParam(parent: Params,
                           name: String,
                           doc: String,
                           isValid: Seq[TextAnalyzeTask] => Boolean = (_: Seq[TextAnalyzeTask]) => true)
                          (@transient override implicit val dataFormat: JsonFormat[Seq[TextAnalyzeTask]])
  extends CognitiveServiceStructParam[Seq[TextAnalyzeTask]](parent, name, doc, isValid) {
  type ValueType = TextAnalyzeTask

  override def w(value: Seq[TextAnalyzeTask]): ParamPair[Seq[TextAnalyzeTask]] = super.w(value)

  def w(value: java.util.ArrayList[util.HashMap[String, Any]]): ParamPair[Seq[TextAnalyzeTask]] =
    super.w(value.asScala.toArray.map(hashMapToTAAnalyzeTask))

  def hashMapToTAAnalyzeTask(value: util.HashMap[String, Any]): TextAnalyzeTask = {
    if (!value.containsKey("parameters")) {
      throw new IllegalArgumentException("Task optiosn must include 'parameters' value")
    }
    if (value.size() > 1) {
      throw new IllegalArgumentException("Task options should only include 'parameters' value")
    }
    val valParameters = value.get("parameters").asInstanceOf[util.HashMap[String, Any]]
    val parameters = valParameters.asScala.toMap.map { x => (x._1, x._2.toString) }
    TextAnalyzeTask(parameters)
  }
}

object TextAnalyze extends ComplexParamsReadable[TextAnalyze]

class TextAnalyze(override val uid: String) extends TextAnalyticsBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation
  with HasSetLinkedService with BasicAsyncReply {

  import TAJSONFormat._

  def this() = this(Identifiable.randomUID("TextAnalyze"))

  val entityRecognitionTasks = new TextAnalyzeTaskParam(
    this,
    "entityRecognitionTasks",
    "the entity recognition tasks to perform on submitted documents"
  )

  def getEntityRecognitionTasks: Seq[TextAnalyzeTask] = $(entityRecognitionTasks)

  def setEntityRecognitionTasks(v: Seq[TextAnalyzeTask]): this.type = set(entityRecognitionTasks, v)

  setDefault(entityRecognitionTasks -> Seq[TextAnalyzeTask]())

  val entityRecognitionPiiTasks = new TextAnalyzeTaskParam(
    this,
    "entityRecognitionPiiTasks",
    "the entity recognition pii tasks to perform on submitted documents"
  )

  def getEntityRecognitionPiiTasks: Seq[TextAnalyzeTask] = $(entityRecognitionPiiTasks)

  def setEntityRecognitionPiiTasks(v: Seq[TextAnalyzeTask]): this.type = set(entityRecognitionPiiTasks, v)

  setDefault(entityRecognitionPiiTasks -> Seq[TextAnalyzeTask]())

  val entityLinkingTasks = new TextAnalyzeTaskParam(
    this,
    "entityLinkingTasks",
    "the entity linking tasks to perform on submitted documents"
  )

  def getEntityLinkingTasks: Seq[TextAnalyzeTask] = $(entityLinkingTasks)

  def setEntityLinkingTasks(v: Seq[TextAnalyzeTask]): this.type = set(entityLinkingTasks, v)

  setDefault(entityLinkingTasks -> Seq[TextAnalyzeTask]())

  val keyPhraseExtractionTasks = new TextAnalyzeTaskParam(
    this,
    "keyPhraseExtractionTasks",
    "the key phrase extraction tasks to perform on submitted documents"
  )

  def getKeyPhraseExtractionTasks: Seq[TextAnalyzeTask] = $(keyPhraseExtractionTasks)

  def setKeyPhraseExtractionTasks(v: Seq[TextAnalyzeTask]): this.type = set(keyPhraseExtractionTasks, v)

  setDefault(keyPhraseExtractionTasks -> Seq[TextAnalyzeTask]())

  val sentimentAnalysisTasks = new TextAnalyzeTaskParam(
    this,
    "sentimentAnalysisTasks",
    "the sentiment analysis tasks to perform on submitted documents"
  )

  def getSentimentAnalysisTasks: Seq[TextAnalyzeTask] = $(sentimentAnalysisTasks)

  def setSentimentAnalysisTasks(v: Seq[TextAnalyzeTask]): this.type = set(sentimentAnalysisTasks, v)

  setDefault(sentimentAnalysisTasks -> Seq[TextAnalyzeTask]())

  override protected def responseDataType: StructType = TextAnalyzeResponse.schema

  def urlPath: String = "/text/analytics/v3.1/analyze"

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }

  override protected def modifyPollingURI(originalURI: URI): URI = {
    // async API allows up to 25 results to be submitted in a batch, but defaults to 20 results per page
    // Add $top=25 to force the full batch in the response
    val originalQuery = originalURI.getQuery
    val newQuery = originalQuery match {
      case "" => "$top=25"
      case _ => "$top=25&" + originalQuery // The API picks up the first value of top so add as a prefix
    }
    new URI(
      originalURI.getScheme,
      originalURI.getUserInfo,
      originalURI.getHost,
      originalURI.getPort,
      originalURI.getPath,
      newQuery,
      originalURI.getFragment
    )
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else if (getValue(row, text).forall(Option(_).isEmpty)) {
        None
      } else {
        import TAJSONFormat._
        val post = new HttpPost(getUrl)
        getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
        post.setHeader("Content-Type", "application/json")
        val texts = getValue(row, text)

        val languages: Option[Seq[String]] = (getValueOpt(row, language) match {
          case Some(Seq(lang)) => Some(Seq.fill(texts.size)(lang))
          case s => s
        })

        val documents = texts.zipWithIndex.map { case (t, i) =>
          TADocument(languages.flatMap(ls => Option(ls(i))), i.toString, Option(t).getOrElse(""))
        }
        val displayName = "SynapseML"
        val analysisInput = TextAnalyzeInput(documents)
        val tasks = TextAnalyzeTasks(
          entityRecognitionTasks = getEntityRecognitionTasks,
          entityLinkingTasks = getEntityLinkingTasks,
          entityRecognitionPiiTasks = getEntityRecognitionPiiTasks,
          keyPhraseExtractionTasks = getKeyPhraseExtractionTasks,
          sentimentAnalysisTasks = getSentimentAnalysisTasks
        )
        val json = TextAnalyzeRequest(displayName, analysisInput, tasks).toJson.compactPrint
        post.setEntity(new StringEntity(json, "UTF-8"))
        Some(post)
      }
    }
  }

  private def getTaskRowsTyped[T <: HasDocId](namedTaskRow: Seq[TextAnalyzeAPIResults[T]],
                                              documentIndex: Int): Seq[TextAnalyzeResult[T]] = {
    namedTaskRow.map(inputRow => {
      val state = inputRow.state
      if (inputRow.results.isDefined && state.toLowerCase == "succeeded") {
        val result = inputRow.results.get
        val documents = result.documents
        val errors = result.errors
        val doc = documents.find { d => d.id.toInt == documentIndex }
        val error = errors.find { e => e.id.toInt == documentIndex }
        TextAnalyzeResult(doc, error) // result/errors per task, per document
      } else {
        // Task failed
        val doc: Option[T] = None
        TextAnalyzeResult(doc, Some(TAError(documentIndex.toString, s"Task failed with state $state")))
      }
    })
  }

  override protected def unpackBatchUDF: UserDefinedFunction = {
    val innerResponseDataType = TextAnalyzeSimplifiedResponse.schema
    val fromRow = TextAnalyzeResponse.makeFromRowConverter
    val toRow = TextAnalyzeSimplifiedResponse.makeToRowConverter
    UDFUtils.oldUdf({ rowOpt: Row =>
      Option(rowOpt).map { row =>
        val parsed = fromRow(row)
        val allTasks = Seq(
          parsed.tasks.entityRecognitionTasks,
          parsed.tasks.entityLinkingTasks,
          parsed.tasks.entityRecognitionPiiTasks,
          parsed.tasks.keyPhraseExtractionTasks,
          parsed.tasks.sentimentAnalysisTasks
        )

        val succeededTasks = allTasks
          .flatten
          .flatten
          // only consider tasks that succeeded to handle 'partiallycompleted' requests
          .filter(_.state.toLowerCase == "succeeded")
          .filter(_.results.isDefined)

        if (succeededTasks.isEmpty) {
          Seq()
        } else {
          val results = succeededTasks.head.results.get
          val docCount = results.documents.size
          val errorCount = results.errors.size
          (0 until (docCount + errorCount)).map(i => {
            toRow(TextAnalyzeSimplifiedResponse(
              parsed.tasks.entityRecognitionTasks.map(t => getTaskRowsTyped(t, i)),
              parsed.tasks.entityLinkingTasks.map(t => getTaskRowsTyped(t, i)),
              parsed.tasks.entityRecognitionPiiTasks.map(t => getTaskRowsTyped(t, i)),
              parsed.tasks.keyPhraseExtractionTasks.map(t => getTaskRowsTyped(t, i)),
              parsed.tasks.sentimentAnalysisTasks.map(t => getTaskRowsTyped(t, i))
            ))
          })
        }
      }
    }, ArrayType(innerResponseDataType)
    )
  }

}
