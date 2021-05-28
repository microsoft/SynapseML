package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.StringArrayParam
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class TabularLIME(override val uid: String)
  extends LIMEBase(uid)
    with HasInputCols {

  def this() = {
    this(Identifiable.randomUID("tab_lime"))
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
                                       featureStats: Seq[FeatureStats],
                                       idCol: String,
                                       distanceCol: String): DataFrame = {

    val numSamples = this.getNumSamples

    val sampler = new LIMETabularSampler(featureStats)

    val sampleType = StructType(featureStats.map {
      feature =>
        val name = df.schema(feature.fieldIndex).name
        StructField(name, DoubleType)
    })

    val returnDataType = ArrayType(
      StructType(Seq(
        StructField("sample", sampleType),
        StructField("distance", DoubleType)
      ))
    )

    val samplesUdf = UDFUtils.oldUdf(
      {
        row: Row =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          (1 to numSamples).map {
            _ =>
              val (sample, distance) = sampler.sample(row)
              (sample, distance)
          }
      },
      returnDataType
    )

    df.withColumn("samples", explode(samplesUdf(struct(df.columns.map(col): _*))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.sample.*")
      )
  }

  override protected def row2Vector(row: Row): BDV[Double] = {
    BDV(this.getInputCols.map(row.getAsDouble))
  }

  import spark.implicits._

  override protected def createFeatureStats(df: DataFrame): Seq[FeatureStats] = {
    val categoryFeatures = this.getInputCols.filter(this.getCategoricalFeatures.contains)
    val numericFeatures = this.getInputCols.filterNot(this.getCategoricalFeatures.contains)

    val maxFeatureMembers: Int = 1000
    val categoryFeatureStats = categoryFeatures.par.map {
      feature =>
        val freqMap = df.select(col(feature).cast(DoubleType).alias(feature))
          .groupBy(feature)
          .agg(count("*").cast(DoubleType).alias("count"))
          .sort($"count".desc)
          .as[(Double, Double)]
          .head(maxFeatureMembers)
          .toMap

        val fieldIndex = df.schema.fieldIndex(feature)
        DiscreteFeatureStats(fieldIndex, freqMap)
    }

    val numericAggregates = numericFeatures.map(f => stddev(f).cast(DoubleType).alias(f))

    val numFeatureStats = if (numericAggregates.nonEmpty) {
      val row = df.agg(numericAggregates.head, numericAggregates.tail: _*).head

      numericFeatures.map {
        feature =>
          val stddev = row.getAs[Double](feature)
          val fieldIndex = df.schema.fieldIndex(feature)
          ContinuousFeatureStats(fieldIndex, stddev)
      }.toSeq
    } else {
      Seq.empty
    }

    categoryFeatureStats.toArray ++: numFeatureStats
  }

  override protected def validateInputSchema(schema: StructType): Unit = {
    super.validateInputSchema(schema)

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
