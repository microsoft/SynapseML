// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.io.http.{ErrorUtils, SimpleHTTPTransformer}
import com.microsoft.ml.spark.io.powerbi.StreamMaterializer
import com.microsoft.ml.spark.stages.{FixedMiniBatchTransformer, HasBatchSize, Lambda}
import org.apache.http.Consts
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.internal.{Logging => SLogging}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel}
import org.apache.spark.sql.functions.{col, expr, struct, to_json, udf}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.microsoft.ml.spark.cognitive.AzureSearchProtocol._
import spray.json._
import DefaultJsonProtocol._
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils

import scala.collection.JavaConverters._

object AddDocuments extends ComplexParamsReadable[AddDocuments] with Serializable

trait HasActionCol extends HasServiceParams {

  val actionCol = new Param[String](this, "actionCol",
    s"""
       |You can combine actions, such as an upload and a delete, in the same batch.
       |
       |upload: An upload action is similar to an 'upsert'
       |where the document will be inserted if it is new and updated/replaced
       |if it exists. Note that all fields are replaced in the update case.
       |
       |merge: Merge updates an existing document with the specified fields.
       |If the document doesn't exist, the merge will fail. Any field
       |you specify in a merge will replace the existing field in the document.
       |This includes fields of type Collection(Edm.String). For example, if
       |the document contains a field 'tags' with value ['budget'] and you execute
       |a merge with value ['economy', 'pool'] for 'tags', the final value
       |of the 'tags' field will be ['economy', 'pool'].
       | It will not be ['budget', 'economy', 'pool'].
       |
       |mergeOrUpload: This action behaves like merge if a document
       | with the given key already exists in the index.
       | If the document does not exist, it behaves like upload with a new document.
       |
       |delete: Delete removes the specified document from the index.
       | Note that any field you specify in a delete operation,
       | other than the key field, will be ignored. If you want to
       |  remove an individual field from a document, use merge
       |  instead and simply set the field explicitly to null.
    """.stripMargin.replace("\n", " ").replace("\r", " "))

  def setActionCol(v: String): this.type = set(actionCol, v)

  def getActionCol: String = $(actionCol)

}

trait HasIndexName extends HasServiceParams {

  val indexName = new Param[String](this, "indexName", "")

  def setIndexName(v: String): this.type = set(indexName, v)

  def getIndexName: String = $(indexName)

}

trait HasServiceName extends HasServiceParams {

  val serviceName = new Param[String](this, "serviceName", "")

  def setServiceName(v: String): this.type = set(serviceName, v)

  def getServiceName: String = $(serviceName)

}

class AddDocuments(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser
  with HasActionCol with HasServiceName with HasIndexName with HasBatchSize
  with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AddDocuments"))

  setDefault(actionCol -> "@search.action")

  override val subscriptionKeyHeaderName = "api-key"

  setDefault(batchSize -> 100)

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val stages = Array(
      Lambda(df =>
        df.withColumnRenamed(getActionCol, "@search.action")
          .select(struct("*").alias("arr"))
      ),
      new FixedMiniBatchTransformer().setBuffered(false).setBatchSize(getBatchSize),
      Lambda(df =>
        df.select(struct(
          to_json(struct(col("arr").alias("value")), Map("charset" -> "UTF-8"))
        ).alias("input"))
      ),
      new SimpleHTTPTransformer()
        .setInputCol("input")
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(get(concurrentTimeout))
        .setErrorCol(getErrorCol)
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      if (get(url).isEmpty) {
        setUrl(s"https://$getServiceName.search.windows.net" +
          s"/indexes/$getIndexName/docs/index?api-version=${AzureSearchAPIConstants.DefaultAPIVersion}")
      }
      super.transform(dataset)
    })
  }

  override def prepareEntity: Row => Option[AbstractHttpEntity] = row =>
    Some(new StringEntity(row.getString(0), ContentType.create("text/plain", Consts.UTF_8)))

  override def responseDataType: DataType = ASResponses.schema
}

object AzureSearchWriter extends IndexParser with SLogging {

  val Logger: Logger = LogManager.getRootLogger

  private def checkForErrors(
                              fatal: Boolean)(errorRow: Row, inputRow: Row): Option[Row] = {
    Option(errorRow).map { r =>
      val message = s"Service Exception:\n\t ${r.toString()} \n for input:\n\t ${inputRow.toString()}"
      if (fatal) {
        throw new RuntimeException(message)
      } else {
        logWarning(message)
        r
      }
    }
  }

  private def filterOutNulls(df: DataFrame, collectionColName: String): DataFrame = {
    df.withColumn(collectionColName, expr(s"filter($collectionColName, x -> x is not null)"))
  }

  private def convertFields(fields: Seq[StructField],
                            keyCol: String,
                            searchActionCol: String,
                            prefix: Option[String]): Seq[IndexField] = {
    fields.filterNot(_.name == searchActionCol).map { sf =>
      val fullName = prefix.map(_ + sf.name).getOrElse(sf.name)
      val (innerType, innerFields) = sparkTypeToEdmType(sf.dataType)
      IndexField(
        sf.name,
        innerType,
        None, None, None, None, None,
        if (keyCol == fullName) Some(true) else None,
        None, None, None, None,
        structFieldToSearchFields(sf.dataType,
          keyCol, searchActionCol, prefix=Some(prefix.getOrElse("") + sf.name + "."))
      )
    }
  }

  private def structFieldToSearchFields(schema: DataType,
                                        keyCol: String,
                                        searchActionCol: String,
                                        prefix: Option[String] = None
                                       ): Option[Seq[IndexField]] = {
    schema match {
      case StructType(fields) => Some(convertFields(fields, keyCol, searchActionCol, prefix))
      case ArrayType(StructType(fields), _) => Some(convertFields(fields, keyCol, searchActionCol, prefix))
      case _ => None
    }
  }

  private def dfToIndexJson(schema: StructType,
                            indexName: String,
                            keyCol: String,
                            searchActionCol: String): String = {
    val is = IndexInfo(
      Some(indexName),
      structFieldToSearchFields(schema, keyCol, searchActionCol).get,
      None, None, None, None, None, None, None, None
    )
    is.toJson.compactPrint
  }

  private def prepareDF(df: DataFrame, options: Map[String, String] = Map()): DataFrame = {
    val applicableOptions = Set(
      "subscriptionKey", "actionCol", "serviceName", "indexName", "indexJson",
      "apiVersion", "batchSize", "fatalErrors", "filterNulls", "keyCol"
    )

    options.keys.foreach(k =>
      assert(applicableOptions(k), s"$k not an applicable option ${applicableOptions.toList}"))

    val subscriptionKey = options("subscriptionKey")
    val actionCol = options.getOrElse("actionCol", "@search.action")
    val serviceName = options("serviceName")
    val indexJsonOpt = options.get("indexJson")
    val apiVersion = options.getOrElse("apiVersion", AzureSearchAPIConstants.DefaultAPIVersion)
    val batchSize = options.getOrElse("batchSize", "100").toInt
    val fatalErrors = options.getOrElse("fatalErrors", "true").toBoolean
    val filterNulls = options.getOrElse("filterNulls", "false").toBoolean

    val keyCol = options.get("keyCol")
    val indexName = options.getOrElse("indexName", parseIndexJson(indexJsonOpt.get).name.get)
    if (indexJsonOpt.isDefined) {
      List("keyCol", "indexName").foreach(opt =>
        assert(options.get(opt).isEmpty, s"Cannot set both indexJson options and $opt")
      )
    }

    val indexJson = indexJsonOpt.getOrElse {
      dfToIndexJson(df.schema, indexName, keyCol.get, actionCol)
    }

    SearchIndex.createIfNoneExists(subscriptionKey, serviceName, indexJson, apiVersion)

    logInfo("checking schema parity")
    checkSchemaParity(df.schema, indexJson, actionCol)

    val df1 = if (filterNulls) {
      val collectionColumns = parseIndexJson(indexJson).fields
        .filter(_.`type`.startsWith("Collection"))
        .map(_.name)
      collectionColumns.foldLeft(df) { (ndf, c) => filterOutNulls(ndf, c) }
    } else {
      df
    }

    new AddDocuments()
      .setSubscriptionKey(subscriptionKey)
      .setServiceName(serviceName)
      .setIndexName(indexName)
      .setActionCol(actionCol)
      .setBatchSize(batchSize)
      .setOutputCol("out")
      .setErrorCol("error")
      .transform(df1)
      .withColumn("error",
        UDFUtils.oldUdf(checkForErrors(fatalErrors) _, ErrorUtils.ErrorSchema)(col("error"), col("input")))
  }

  private def isEdmCollection(t: String): Boolean = {
    t.startsWith("Collection(") && t.endsWith(")")
  }

  private def getEdmCollectionElement(t: String): String = {
    t.substring("Collection(".length).dropRight(1)
  }

  private[ml] def edmTypeToSparkType(dt: String,
                                     fields: Option[Seq[IndexField]]): DataType = dt match {
    case t if isEdmCollection(t) =>
      ArrayType(edmTypeToSparkType(getEdmCollectionElement(t), fields), containsNull = false)
    case "Edm.String" => StringType
    case "Edm.Boolean" => BooleanType
    case "Edm.Int64" => LongType
    case "Edm.Int32" => IntegerType
    case "Edm.Double" => DoubleType
    case "Edm.DateTimeOffset" => StringType //See if there's a way to use spark datetimes
    case "Edm.GeographyPoint" => StringType
    case "Edm.ComplexType" => StructType(fields.get.map(f =>
      StructField(f.name, edmTypeToSparkType(f.`type`, f.fields))))
  }

  private def sparkTypeToEdmType(dt: DataType,
                                 allowCollections: Boolean = true): (String, Option[Seq[IndexField]]) = {
    dt match {
      case ArrayType(it, _) if allowCollections =>
        val (innerType, innerFields) = sparkTypeToEdmType(it, allowCollections = false)
        (s"Collection($innerType)", innerFields)
      case ArrayType(it, _) if !allowCollections =>
        val (innerType, innerFields) = sparkTypeToEdmType(it, allowCollections)
        ("Edm.ComplexType", innerFields)
      case StringType => ("Edm.String", None)
      case BooleanType => ("Edm.Boolean", None)
      case IntegerType => ("Edm.Int32", None)
      case LongType => ("Edm.Int64", None)
      case DoubleType => ("Edm.Double", None)
      case DateType => ("Edm.DateTimeOffset", None)
      case StructType(fields) => ("Edm.ComplexType", Some(fields.map{f=>
        val (innerType, innerFields) = sparkTypeToEdmType(f.dataType)
        IndexField(f.name, innerType,None, None, None, None, None, None, None, None, None, None, innerFields)
      }))
    }
  }

  private def dtEqualityModuloNullability(dt1: DataType, dt2: DataType): Boolean = (dt1, dt2) match {
    case (ArrayType(it1, _), ArrayType(it2, _)) => dtEqualityModuloNullability(it1, it2)
    case (StructType(fields1), StructType(fields2)) =>
      fields1.zip(fields2).forall {
        case (sf1, sf2) => sf1.name==sf2.name && dtEqualityModuloNullability(sf1.dataType, sf2.dataType)
      }
    case _ => dt1 == dt2
  }

  private def checkSchemaParity(schema: StructType, indexJson: String, searchActionCol: String): Unit = {
    val indexInfo = parseIndexJson(indexJson)
    val indexFields = indexInfo.fields.map(f => (f.name, edmTypeToSparkType(f.`type`, f.fields))).toMap

    assert(schema(searchActionCol).dataType == StringType)
    schema.toList.filter(_.name != searchActionCol).foreach { field =>

      val indexType = indexFields.getOrElse(field.name, throw new IllegalArgumentException(
        s"${field.name} not found in search index fields: ${indexFields.keys.toList}"))

      assert(dtEqualityModuloNullability(indexType, field.dataType), s"field ${field.name} requires type" +
        s" $indexType your dataframe column is of type ${field.dataType}")
    }
  }

  def stream(df: DataFrame, options: Map[String, String] = Map()): DataStreamWriter[Row] = {
    prepareDF(df, options).writeStream.foreach(new StreamMaterializer)
  }

  def write(df: DataFrame, options: Map[String, String] = Map()): Unit = {
    prepareDF(df, options).foreach(_ => ())
  }

  def stream(df: DataFrame, options: java.util.HashMap[String, String]): DataStreamWriter[Row] = {
    stream(df, options.asScala.toMap)
  }

  def write(df: DataFrame, options: java.util.HashMap[String, String]): Unit = {
    write(df, options.asScala.toMap)
  }

}
