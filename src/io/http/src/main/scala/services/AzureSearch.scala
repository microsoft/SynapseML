// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.cognitive._
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{NamespaceInjections, PipelineModel}
import org.apache.spark.sql.functions.{array, col, struct, to_json}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row}

import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}

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
    """.stripMargin.replace("\n",""))

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
  with HasActionCol with HasServiceName with HasIndexName with HasBatchSize {

  def this() = this(Identifiable.randomUID("AddDocuments"))

  setDefault(actionCol -> "@search.action")

  override val subscriptionKeyHeaderName = "api-key"

  setDefault(batchSize->100)

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val stages = Array(
      Lambda(df =>
        df.withColumnRenamed(getActionCol, "@search.action")
          .select(struct("*").alias("arr"))
      ),
      new FixedMiniBatchTransformer().setBuffered(false).setBatchSize(getBatchSize),
      Lambda(df =>
        df.select(struct(
          to_json(struct(col("arr").alias("value")))
        ).alias("input"))
      ),
      new SimpleHTTPTransformer()
        .setInputCol("input")
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(getConcurrentTimeout)
        .setErrorCol(getErrorCol)
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (get(url).isEmpty) {
      setUrl(s"https://$getServiceName.search.windows.net" +
        s"/indexes/$getIndexName/docs/index?api-version=2017-11-11")
    }
    super.transform(dataset)
  }

  override def prepareEntity: Row => Option[AbstractHttpEntity] = { row =>
    Some(new StringEntity(row.getString(0)))
  }

  override def responseDataType: DataType = ASResponses.schema
}

private[ml] class StreamMaterializer2 extends ForeachWriter[Row] {

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: Row): Unit = ()

  override def close(errorOrNull: Throwable): Unit = ()

}

object AzureSearchWriter extends IndexParser {

  val logger: Logger = LogManager.getRootLogger

  private def prepareDF(df: DataFrame, options: Map[String, String] = Map()): DataFrame = {
    val applicableOptions = Set(
      "subscriptionKey", "actionCol", "serviceName", "indexName", "indexJson",
      "apiVersion", "batchSize"
    )

    options.keys.foreach(k =>
      assert(applicableOptions(k), s"$k not an applicable option ${applicableOptions.toList}"))

    val subscriptionKey = options("subscriptionKey")
    val actionCol = options.getOrElse("actionCol", "@search.action")
    val serviceName = options("serviceName")
    val indexJson = options("indexJson")
    val apiVersion = options.getOrElse("apiVersion", "2017-11-11")
    val indexName = parseIndexJson(indexJson).name.get
    val batchSize = options.getOrElse("batchSize", "100").toInt

    SearchIndex.createIfNoneExists(subscriptionKey,serviceName, indexJson, apiVersion)

    checkSchemaParity(df, indexJson) match {
      case Success(_) => ()
      case Failure(e) =>
        println("Exception: Schema mismatch found in dataframe and json")
        throw e
    }

    new AddDocuments()
      .setSubscriptionKey(subscriptionKey)
      .setServiceName(serviceName)
      .setIndexName(indexName)
      .setActionCol(actionCol)
      .setBatchSize(batchSize)
      .transform(df)
  }

  private def checkSchemaParity(df: DataFrame, indexJson: String): Try[Boolean] = {
    val edmTypes = Map("Edm.String" -> "string",
      "Collection(Edm.String)" -> "array<string>",
      "Edm.Boolean" -> "boolean",
      "Edm.Int64" -> "bigint",
      "Edm.Int32" -> "int",
      "Edm.Double" -> "double",
      "Edm.DateTimeOffset" -> "string",
      "Edm.GeographyPoint" -> "string")
    val fieldNames = parseIndexJson(indexJson).fields.map(f => f.name).toList.map(n => n.toString)
    val fieldTypes = parseIndexJson(indexJson).fields.map(f => f.`type`).toList.map(t => edmTypes(t.toString))

    // drop the first comparison element because the first field in the dataframe corresponds to the search action
    val isValid = df.schema.toList.drop(1).map(field =>
        fieldNames.contains(field.name) && fieldTypes.contains(field.dataType.simpleString)
      )

    val result = isValid.forall(x => x)

    if (result) {Success(result)}
    else {Failure(new IllegalArgumentException)}
  }

  def stream(df: DataFrame, options: Map[String, String] = Map()): DataStreamWriter[Row] = {
    prepareDF(df, options).writeStream.foreach(new StreamMaterializer2)
  }

  def write(df: DataFrame, options: Map[String, String] = Map()): Unit = {
    prepareDF(df, options).foreachPartition(it => it.foreach(_ => ()))
  }

  def stream(df: DataFrame, options: java.util.HashMap[String, String]): DataStreamWriter[Row] = {
    stream(df, options.asScala.toMap)
  }

  def write(df: DataFrame,
            options: java.util.HashMap[String, String]): Unit = {
    write(df, options.asScala.toMap)
  }

}
