// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.execution.streaming

import javax.annotation.concurrent.GuardedBy

import com.microsoft.ml.spark.{BingImage, BingImageSearch, BingImagesResponse, Lambda}
import org.apache.spark.SparkContext
import org.apache.spark.ml.NamespaceInjections
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}
import org.apache.spark.sql.functions.{col, explode, lit, udf}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class CountingSource(batchSize: Int,
                     sqlContext: SQLContext)
  extends Source {

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOrdinal =
      start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset.toInt + 1

    val rawList = (startOrdinal * batchSize until endOrdinal * batchSize).map{i =>
      val row = new GenericInternalRow(1)
      row.update(0, i.toLong)
      row.asInstanceOf[InternalRow]
    }
    val rawBatch = sqlContext.sparkContext.parallelize(rawList)
    sqlContext.sparkSession
      .internalCreateDataFrame(rawBatch, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = ()

  override def toString: String = s"CountingSource"

  override def schema: StructType = new StructType().add("count", LongType)

  override def stop(): Unit = ()

  override def getOffset: Option[Offset] = {
    currentOffset += 1
    Some(currentOffset)
  }
}

class CountSourceProvider extends StreamSourceProvider with DataSourceRegister {

  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    ("CountingSource", new StructType().add("count", LongType))
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    new CountingSource(parameters("batchSize").toInt, sqlContext)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "CountingSource"

}

class BingImageSource(searchTerms: Seq[String],
                      key: String,
                      url: String,
                      batchSize: Int = 10,
                      imgsPerBatch: Int = 10) {

  def load(spark: SparkSession): DataFrame = {
    val df = spark
      .readStream.format(classOf[CountSourceProvider].getName)
      .option("batchSize", batchSize.toLong)
      .load()
      .select(col("count").multiply(imgsPerBatch).alias("offset"))
      .withColumn("searchTerm", explode(lit(searchTerms.toArray)))

    val bis: BingImageSearch = new BingImageSearch()
      .setSubscriptionKey(key)
      .setUrl(url)
      .setVectorParam("q", "searchTerm")
      .setVectorParam("offset","offset")
      .setScalarParam("count", imgsPerBatch)
      .setOutputCol("images")

    val fromRow = BingImagesResponse.makeFromRowConverter
    val getUrlUDF: UserDefinedFunction = udf(
      { r: Row => fromRow(r).value},  ArrayType(BingImage.schema))
    val getUrls = Lambda(_
      .withColumn("image", explode(getUrlUDF(col(bis.getOutputCol))))
      .select("searchTerm", "image")
    )

    NamespaceInjections
      .pipelineModel(Array(bis, getUrls))
      .transform(df)
  }

  def this(searchTerms: java.util.ArrayList[String],
    key: java.lang.String, url:java.lang.String, batchSize: java.lang.Integer, imgsPerBatch: java.lang.Integer) ={
    this(searchTerms.toSeq, key, url, batchSize, imgsPerBatch)
  }

}
