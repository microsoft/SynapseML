// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize.text

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

object PageSplitter extends DefaultParamsReadable[PageSplitter]

/** Splits text into chunks of at most n characters
  *
  * @param uid The id of the module
  */
class PageSplitter(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol
    with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Featurize)

  def this() = this(Identifiable.randomUID("PageSplitter"))

  setDefault(outputCol, uid + "_output")

  val maximumPageLength =
    new IntParam(this, "maximumPageLength",
      "the maximum number of characters to be in a page")

  def setMaximumPageLength(v: Int): this.type = set(maximumPageLength, v)

  def getMaximumPageLength: Int = $(maximumPageLength)

  val minimumPageLength =
    new IntParam(this, "minimumPageLength",
      "the the minimum number of characters " +
        "to have on a page in order to preserve work boundaries")

  def setMinimumPageLength(v: Int): this.type = set(minimumPageLength, v)

  def getMinimumPageLength: Int = $(minimumPageLength)

  val boundaryRegex = new Param[String](this, "boundaryRegex", "how to split into words")

  def setBoundaryRegex(v: String): this.type = set(boundaryRegex, v)

  def getBoundaryRegex: String = $(boundaryRegex)

  setDefault(maximumPageLength -> 5000, minimumPageLength -> 4500, boundaryRegex -> "\\s")

  def split(textOpt: String): Seq[String] = {
    Option(textOpt).map { text =>
      if (text.length < getMaximumPageLength) {
        Seq(text)
      } else {
        val lengths = text
          .split(getBoundaryRegex)
          .map(_.length)
          .flatMap(l => List(l, 1))
          .dropRight(1)

        val indicies = lengths.scanLeft((0, 0, Nil: List[Int])) { case ((total, count, _), l) =>
          if (count + l < getMaximumPageLength) {
            (total + l, count + l, Nil)
          } else if (count > getMinimumPageLength) {
            (total + l, l, List(total))
          } else {
            val firstPageChars = getMaximumPageLength - count
            val firstPage = firstPageChars + total
            val remainingChars = l - firstPageChars

            val numPages = remainingChars / getMaximumPageLength
            val remainder = remainingChars - getMaximumPageLength * numPages
            val pages = List(firstPage) ::: (1 to numPages).map(i =>
              total + firstPageChars + getMaximumPageLength * i).toList
            (total + l, remainder, pages)
          }
        }.flatMap(_._3)

        val words = (List(0) ::: indicies.toList ::: List(text.length))
          .sliding(2)
          .map { case List(start, end) => text.substring(start, end) }
          .toSeq
        words
      }
    }.orNull
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      dataset.toDF().withColumn(getOutputCol, UDFUtils.oldUdf(split _, ArrayType(StringType))(col(getInputCol))),
      dataset.columns.length
    )
  }

  override def copy(extra: ParamMap): PageSplitter =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == StringType)
    schema.add(getOutputCol, ArrayType(StringType))
  }

}
