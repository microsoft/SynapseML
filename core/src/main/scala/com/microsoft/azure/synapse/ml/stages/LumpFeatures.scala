// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import spray.json.DefaultJsonProtocol.IntJsonFormat

object LumpFeatures extends DefaultParamsReadable[LumpFeatures]

/** <code>LumpFeatures</code> takes a dataframe and a list of lumping rules as input and returns
  * a dataframe comprised of the original columns but the columns defined in lumping rules
  * will be indexed and lumped to top k.
  *
  * This transformer can be used to handle high cardinality skewed categorical before doing encoding.
  *
  */

class LumpFeatures(val uid: String) extends Transformer with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass()

  def this() = this(Identifiable.randomUID("LumpCategoricalFeatures"))

  val lumpRules: Param[String] = new Param(this, "lumpRules", "JSON string representing lumping rules")

  def getLumpRules: String = $(lumpRules)

  def setLumpRules(value: String): this.type = set(lumpRules, value)

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from categorical features lumping
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val lumpMap = spray.json.JsonParser(getLumpRules).asJsObject.fields.mapValues(_.convertTo[Int])

      val indexers = lumpMap.keys.map { colName =>
        new StringIndexer()
          .setInputCol(colName)
          .setOutputCol(colName + "_indexed")
          .setHandleInvalid("keep")
      }

      val pipeline = new Pipeline().setStages(indexers.toArray)
      val indexedDF = pipeline.fit(dataset).transform(dataset)

      // Keep top k levels for each categorical column
      val cleanedDFWithLumpedCols = lumpMap.keys.foldLeft(indexedDF) { (df, colName) =>
        val k = lumpMap(colName)
        df.withColumn(colName + "_lumped", when(col(colName + "_indexed") >= k, k).otherwise(col(colName + "_indexed")))
          .withColumn(colName, col(colName + "_lumped").cast("string"))
          .drop(colName + "_indexed", colName + "_lumped")
      }

      cleanedDFWithLumpedCols
    })
  }

  def transformSchema(schema: StructType): StructType = {
    StructType(schema)
  }

  def copy(extra: ParamMap): LumpFeatures = defaultCopy(extra)
}


