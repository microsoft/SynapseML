// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.StringArrayParam
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class TabularLIME(override val uid: String)
  extends LIMEBase(uid)
    with HasInputCols
    with HasBackgroundData {

  logClass()

  def this() = {
    this(Identifiable.randomUID("TabularLIME"))
  }

  override protected lazy val pyInternalWrapper = true

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
                                       distanceCol: String): DataFrame = {

    val numSamples = this.getNumSamples

    val featureStats = this.createFeatureStats(this.backgroundData.getOrElse(df))

    val sampleType = StructType(this.getInputCols.map(StructField(_, DoubleType)))

    val returnDataType = ArrayType(
      StructType(Seq(
        StructField("sample", sampleType),
        StructField("state", SQLDataTypes.VectorType),
        StructField("distance", DoubleType)
      ))
    )

    val samplesUdf = UDFUtils.oldUdf(
      {
        row: Row =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          val sampler = new LIMETabularSampler(row, featureStats)
          (1 to numSamples).map {
            _ =>

              val (sample, feature, distance) = sampler.sample
              (sample, feature, distance)
          }
      },
      returnDataType
    )

    df.withColumn("samples", explode(samplesUdf(struct(getInputCols.map(col): _*))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.state").alias(stateCol),
        col("samples.sample.*")
      )
  }

  private def createFeatureStats(df: DataFrame): Seq[FeatureStats[Double]] = {
    import df.sparkSession.implicits._

    val categoryFeatures = this.getInputCols.filter(this.getCategoricalFeatures.contains)
    val numericFeatures = this.getInputCols.filterNot(this.getCategoricalFeatures.contains)

    val maxFeatureMembers: Int = 1000
    val categoryFeatureStats = categoryFeatures.par.map {
      feature =>
        val freqMap = df.select(col(feature).cast(DoubleType).alias(feature))
          .groupBy(feature)
          .agg(count("*").cast(DoubleType).alias("count"))
          .sort(col("count").desc)
          .as[(Double, Double)]
          .head(maxFeatureMembers)
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

    this.getInputCols.foreach {
      inputCol =>
        require(
          schema(inputCol).dataType.isInstanceOf[NumericType],
          s"Field $inputCol is expected to be numeric type, but got ${schema(inputCol).dataType} instead."
        )
    }

    if (this.backgroundData.nonEmpty) {
      val schema = this.backgroundData.get.schema
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
