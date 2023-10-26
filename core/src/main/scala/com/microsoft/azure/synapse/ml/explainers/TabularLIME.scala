// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.StringArrayParam
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, linalg}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class TabularLIME(override val uid: String)
  extends LIMEBase(uid)
    with HasInputCols
    with HasBackgroundData {

  logClass(FeatureNames.Explainers)

  def this() = {
    this(Identifiable.randomUID("TabularLIME"))
  }

  val categoricalFeatures = new StringArrayParam(
    this,
    "categoricalFeatures",
    "Name of features that should be treated as categorical variables."
  )

  def getCategoricalFeatures: Array[String] = $(categoricalFeatures)

  def setCategoricalFeatures(values: Array[String]): this.type = this.set(categoricalFeatures, values)

  def setInputCols(values: Array[String]): this.type = this.set(inputCols, values)

  setDefault(categoricalFeatures -> Array.empty)

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       stateCol: String,
                                       distanceCol: String,
                                       targetClassesCol: String): DataFrame = {

    val numSamples = this.getNumSamples

    val featureStats = this.createFeatureStats(this.getBackgroundData)

    val sampleType = this.getSampleType(df.schema)

    val samplesUdf = UDFUtils.oldUdf(
      {
        row: Row =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          val sampler = new LIMETabularSampler(row, featureStats)

          // Adding identity sample to avoid all zero states in the sample space for categorical variables.
          sampler.sampleIdentity +: {
            (1 to numSamples).map {
              _ =>
                val (sample: Row, feature: linalg.Vector, distance: Double) = sampler.sample
                (sample, feature, distance)
            }
          }
      },
      getSampleSchema(sampleType)
    )

    val samplesCol = DatasetExtensions.findUnusedColumnName("samples", df)

    df.withColumn(samplesCol, explode(samplesUdf(struct(getInputCols.map(col): _*))))
      .select(
        col(idCol),
        col(samplesCol).getField(distanceField).alias(distanceCol),
        col(samplesCol).getField(stateField).alias(stateCol),
        col(targetClassesCol),
        expr(s"$samplesCol.$sampleField.*")
      )
  }

  private def getSampleType(inputSchema: StructType) = {
    val fields = this.getInputCols map {
      case c if getCategoricalFeatures.contains(c) =>
        inputSchema(c)
      case c =>
        StructField(c, DoubleType)
    }

    StructType(fields)
  }

  private def createFeatureStats(df: DataFrame): Seq[FeatureStats[_]] = {
    val categoryFeatures = this.getInputCols.filter(this.getCategoricalFeatures.contains)
    val numericFeatures = this.getInputCols.filterNot(this.getCategoricalFeatures.contains)
    val countCol = DatasetExtensions.findUnusedColumnName("count", df)
    val maxFeatureMembers: Int = 1000
    val categoryFeatureStats = categoryFeatures.par.map {
      feature =>
        val freqMap = df.groupBy(feature)
          .agg(count("*").cast(DoubleType).alias(countCol))
          .sort(col(countCol).desc)
          .head(maxFeatureMembers)
          .map(row => (row.get(0), row.getDouble(1)))
          .toMap

        (feature, DiscreteFeatureStats(freqMap))
    }

    val numericAggregates = numericFeatures.map(f => stddev(f).cast(DoubleType).alias(f))

    val numFeatureStats = if (numericAggregates.nonEmpty) {
      val row = df.agg(numericAggregates.head, numericAggregates.tail: _*).head

      numericFeatures.map {
        feature =>
          val stddev = row.getAs[Double](feature)
          (feature, ContinuousFeatureStats(stddev))
      }.toSeq
    } else {
      Seq.empty
    }

    val stats = (categoryFeatureStats.toArray ++: numFeatureStats).toMap

    this.getInputCols.map(stats)
  }

  override protected def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    this.getInputCols.filterNot(this.getCategoricalFeatures.contains).foreach {
      inputCol =>
        require(
          schema(inputCol).dataType.isInstanceOf[NumericType],
          s"Field $inputCol is expected to be numeric type, but got ${schema(inputCol).dataType} instead."
        )
    }

    if (this.get(backgroundData).isDefined) {
      val schema = this.getBackgroundData.schema
      this.getInputCols.foreach {
        inputCol =>
          require(
            schema.fieldNames.contains(inputCol),
            s"Field $inputCol not found in background data schema: ${schema.simpleString}")
      }
    }

    val e = this.getCategoricalFeatures.filterNot(this.getInputCols.contains)
    require(
      e.isEmpty,
      s"Categorical features ${e.mkString(",")} are not found in inputCols ${this.getInputCols.mkString(",")}"
    )
  }
}

object TabularLIME extends ComplexParamsReadable[TabularLIME]
