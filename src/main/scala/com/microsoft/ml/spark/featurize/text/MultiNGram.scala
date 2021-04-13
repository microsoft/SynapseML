// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurize.text

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import spray.json.DefaultJsonProtocol._

object MultiNGram extends DefaultParamsReadable[MultiNGram]

/** Extracts several ngrams
  *
  * @param uid The id of the module
  */
class MultiNGram(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with Wrappable with DefaultParamsWritable {
  logInfo(s"Calling $getClass --- telemetry record")

  def this() = this(Identifiable.randomUID("MultiNGram"))

  setDefault(outputCol, uid + "_output")

  val lengths =
    new TypedArrayParam[Int](this, "lengths",
      "the collection of lengths to use for ngram extraction", {x=>true})

  def getLengths: Seq[Int] = $(lengths)

  def setLengths(v: Seq[Int]): this.type = set(lengths, v)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logInfo("Calling function transform --- telemetry record")
    val df = dataset.toDF()
    val intermediateOutputCols = getLengths.map(n =>
      DatasetExtensions.findUnusedColumnName(s"ngram_$n")(dataset.columns.toSet)
    )
    val models = getLengths.zip(intermediateOutputCols).map { case (n, out) =>
      new NGram().setN(n).setInputCol(getInputCol).setOutputCol(out)
    }
    val intermediateDF = NamespaceInjections.pipelineModel(models.toArray).transform(df)
    intermediateDF.map { row =>
      val mergedNGrams = intermediateOutputCols
        .map(col => row.getAs[Seq[String]](col))
        .reduce(_ ++ _)
      Row.merge(row, Row(mergedNGrams))
    }(RowEncoder(intermediateDF.schema.add(getOutputCol, ArrayType(StringType))))
      .drop(intermediateOutputCols: _*)
  }

  override def copy(extra: ParamMap): MultiNGram =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == ArrayType(StringType))
    schema.add(getOutputCol, ArrayType(StringType))
  }

}
