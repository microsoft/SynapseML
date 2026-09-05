// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.param.ServiceParam
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Driver-side, single-writer orchestration with an append-only operation journal.
  * Every accepted operation is committed before polling; each completed unit is
  * committed independently. Spark task retries cannot repeat these driver-side POSTs.
  * A crash between a service POST and its journal commit can still repeat that POST.
  */
object ContentUnderstandingWriter {

  val Schema: StructType = StructType(Seq(
    StructField("documentId", StringType),
    StructField("requestHash", StringType),
    StructField("sequence", LongType)
  ) ++ ContentUnderstandingResponse.schema.fields)

  private val MetadataColumns = Seq("documentId", "requestHash", "sequence", "operationLocation", "status")
  private val TerminalStatuses = Set("Succeeded", "Failed", "Canceled", "Cancelled", "ResultUnavailable")

  private def quoted(name: String): Column = col("`" + name.replace("`", "``") + "`")

  /** Return one latest state per document/range ID, including pending and failed operations. */
  def latest(journal: DataFrame): DataFrame = {
    validateJournal(journal.schema)
    val window = Window.partitionBy("documentId").orderBy(col("sequence").desc)
    journal.withColumn("_cu_rank", row_number().over(window))
      .filter(col("_cu_rank") === lit(1))
      .drop("_cu_rank")
  }

  def readTable(spark: SparkSession, tableName: String): DataFrame = latest(spark.table(tableName))

  def readPath(spark: SparkSession, path: String, format: String = "delta"): DataFrame =
    latest(spark.read.format(format).load(path))

  def writeToTable(dataset: Dataset[_],
                   analyzer: ContentUnderstanding,
                   idCol: String,
                   tableName: String,
                   format: String = "delta",
                   batchSize: Int = 1): DataFrame = {
    require(tableName != null && tableName.trim.nonEmpty, "tableName must not be empty")
    val spark = dataset.sparkSession
    val store = new Journal {
      override def exists: Boolean = spark.catalog.tableExists(tableName)
      override def read(): DataFrame = spark.table(tableName)
      override def append(data: DataFrame): Unit =
        data.write.format(format).mode("append").saveAsTable(tableName)
    }
    write(dataset, analyzer, idCol, format, batchSize, store)
  }

  def writeToPath(dataset: Dataset[_],
                  analyzer: ContentUnderstanding,
                  idCol: String,
                  path: String,
                  format: String = "delta",
                  batchSize: Int = 1): DataFrame = {
    require(path != null && path.trim.nonEmpty, "path must not be empty")
    val spark = dataset.sparkSession
    val output = new Path(path)
    val fs = output.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val store = new Journal {
      override def exists: Boolean = fs.exists(output)
      override def read(): DataFrame = spark.read.format(format).load(path)
      override def append(data: DataFrame): Unit = data.write.format(format).mode("append").save(path)
    }
    write(dataset, analyzer, idCol, format, batchSize, store)
  }

  private trait Journal {
    def exists: Boolean
    def read(): DataFrame
    def append(data: DataFrame): Unit
  }

  private def validateJournal(schema: StructType): Unit = {
    require(
      schema.fields.map(f => f.name -> f.dataType).toSeq == Schema.fields.map(f => f.name -> f.dataType).toSeq,
      "The destination must be a Content Understanding operation journal with the expected schema"
    )
  }

  private def inputColumns(analyzer: ContentUnderstanding): Seq[String] =
    analyzer.params.toSeq.flatMap {
      case p: ServiceParam[_] if analyzer.isDefined(p) => analyzer.getOrDefault(p).right.toOption
      case _ => None
    }

  private def validateInput(dataset: Dataset[_], analyzer: ContentUnderstanding, idCol: String): Unit = {
    require(!dataset.isStreaming, "Use the writer inside foreachBatch for streaming input")
    require(idCol != null && dataset.schema.fieldNames.contains(idCol), "idCol must name an input column")
    require(dataset.schema(idCol).dataType == StringType, "idCol must have StringType")
    require(analyzer.getOperationMode != "poll", "The durable writer requires document input, not poll mode")
    analyzer.validateInputSchema(dataset.schema)
    val ids = dataset.select(quoted(idCol).alias("documentId"))
    require(
      ids.filter(col("documentId").isNull || col("documentId").rlike("(?U)^\\s*$")).limit(1).count() == 0,
      "Document IDs must not be null or blank"
    )
    require(
      ids.groupBy("documentId").count().filter(col("count") > lit(1)).limit(1).count() == 0,
      "Document IDs must be unique; include the selected page/time range in each ID"
    )
  }

  private def append(spark: SparkSession,
                     journal: Journal,
                     documentId: String,
                     requestHash: String,
                     sequence: Long,
                     response: ContentUnderstandingResponse): Unit = {
    val responseRow = ContentUnderstandingResponse.makeToRowConverter(response)
    val record = Row.fromSeq(Seq(documentId, requestHash, sequence) ++ responseRow.toSeq)
    journal.append(spark.createDataFrame(Seq(record).asJava, Schema))
  }

  private def recordSubmission(spark: SparkSession,
                               journal: Journal,
                               documentId: String,
                               requestHash: String,
                               sequence: Long,
                               response: ContentUnderstandingResponse): Unit = {
    if (response.status == "Rejected") {
      throw new ContentUnderstandingException(response)
    }
    append(spark, journal, documentId, requestHash, sequence, response)
    if (!TerminalStatuses(response.status) && response.operationLocation.isEmpty) {
      throw new ContentUnderstandingException(response)
    }
  }

  private def process(spark: SparkSession,
                      journal: Journal,
                      analyzer: ContentUnderstanding,
                      row: Row,
                      documentId: String,
                      previous: Option[Row]): Unit = {
    val requestHash = analyzer.requestFingerprint(row)
    previous.foreach { state =>
      require(state.getAs[String]("requestHash") == requestHash,
        "A document ID was reused with different content or analysis options; use a new ID or a new journal")
    }
    val previousStatus = previous.map(_.getAs[String]("status"))
    if (!previousStatus.exists(TerminalStatuses)) {
      val sequence = previous.map(_.getAs[Long]("sequence")).getOrElse(-1L)
      require(sequence < Long.MaxValue - 1, "The operation journal sequence is exhausted")
      val submitted = previous match {
        case Some(state) =>
          val location = state.getAs[String]("operationLocation")
          val missingHandle = if (state.getAs[String]("status") == "Unknown") {
            "An earlier submission has an unknown outcome and no operation handle. " +
              "Inspect the journal and service outcome before intentionally submitting it with a new ID."
          } else {
            "A pending journal entry is missing its operationLocation"
          }
          require(location != null && location.nonEmpty, missingHandle)
          None
        case None =>
          val response = analyzer.submitOne(row)
          recordSubmission(spark, journal, documentId, requestHash, sequence + 1, response)
          Some(response)
      }
      val location = submitted.flatMap(_.operationLocation)
        .orElse(previous.map(_.getAs[String]("operationLocation")))
      if (!submitted.exists(r => TerminalStatuses(r.status))) {
        require(location.isDefined, "An accepted analysis is missing its operationLocation")
        val response = analyzer.pollOne(row, location.get)
        val nextSequence = if (previous.isDefined) sequence + 1 else sequence + 2
        append(spark, journal, documentId, requestHash, nextSequence, response)
      }
    }
  }

  private def write(dataset: Dataset[_],
                    analyzer: ContentUnderstanding,
                    idCol: String,
                    format: String,
                    batchSize: Int,
                    journal: Journal): DataFrame = {
    require(batchSize > 0, "batchSize must be positive")
    require(Set("delta", "parquet").contains(format),
      "format must be delta or parquet; use Delta for transactional lakehouse tables")
    validateInput(dataset, analyzer, idCol)
    val spark = dataset.sparkSession
    if (journal.exists) {
      validateJournal(journal.read().schema)
    }
    // Check the destination and provider before issuing a billable request.
    journal.append(spark.createDataFrame(Seq.empty[Row].asJava, Schema))
    val selected = (Seq(idCol) ++ inputColumns(analyzer)).distinct.map(quoted)
    val input = dataset.select(selected: _*)
    @tailrec
    def writeBatch(after: Option[String]): Unit = {
      val remaining = after.map(value => input.filter(quoted(idCol) > lit(value))).getOrElse(input)
      val batch = remaining.orderBy(quoted(idCol)).limit(batchSize).collect()
      if (batch.nonEmpty) {
        val ids = batch.map(_.getAs[String](idCol))
        val states = latest(journal.read())
          .filter(col("documentId").isin(ids.toSeq: _*))
          .select(MetadataColumns.map(col): _*)
          .collect()
          .map(state => state.getAs[String]("documentId") -> state).toMap
        batch.foreach { row =>
          val id = row.getAs[String](idCol)
          process(spark, journal, analyzer, row, id, states.get(id))
        }
        writeBatch(Some(ids.last))
      }
    }
    writeBatch(None)
    latest(journal.read())
  }
}
