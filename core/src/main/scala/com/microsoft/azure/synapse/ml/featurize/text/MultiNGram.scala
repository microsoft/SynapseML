// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize.text

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.TypedIntArrayParam
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object MultiNGram extends DefaultParamsReadable[MultiNGram]

/** Extracts several ngrams
  *
  * @param uid The id of the module
  */
class MultiNGram(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol
    with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Featurize)

  def this() = this(Identifiable.randomUID("MultiNGram"))

  setDefault(outputCol, uid + "_output")

  val lengths = new TypedIntArrayParam(
    this,
    "lengths",
    "the collection of lengths to use for ngram extraction"
  )

  def getLengths: Seq[Int] = $(lengths)

  def setLengths(v: Seq[Int]): this.type = set(lengths, v)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
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
        Row.fromSeq(row.toSeq :+ mergedNGrams)
      }(RowEncoder(intermediateDF.schema.add(getOutputCol, ArrayType(StringType))))
        .drop(intermediateOutputCols: _*)
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): MultiNGram =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == ArrayType(StringType))
    schema.add(getOutputCol, ArrayType(StringType))
  }
}
