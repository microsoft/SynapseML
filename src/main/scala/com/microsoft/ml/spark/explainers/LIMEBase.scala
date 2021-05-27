package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector => SV}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait LIMEParams extends HasNumSamples {
  self: LocalExplainer =>

  val regularization = new DoubleParam(
    this,
    "regularization",
    "Regularization param for the lasso. Default value: 0.",
    ParamValidators.gtEq(0)
  )

  val kernelWidth = new DoubleParam(
    this,
    "kernelWidth",
    "Kernel width. Default value: sqrt (number of features) * 0.75",
    ParamValidators.gt(0)
  )

  val metricsCol = new Param[String](
    this,
    "metricsCol",
    "Column name for fitting metrics"
  )

  def getRegularization: Double = $(regularization)

  def setRegularization(v: Double): this.type = set(regularization, v)

  def getKernelWidth: Double = $(kernelWidth)

  def setKernelWidth(v: Double): this.type = set(kernelWidth, v)

  def getMetricsCol: String = $(metricsCol)

  def setMetricsCol(v: String): this.type = this.set(metricsCol, v)

  setDefault(numSamples -> 1000, regularization -> 0.0, kernelWidth -> 0.75, metricsCol -> "r2")
}

abstract class LIMEBase(override val uid: String) extends LocalExplainer with LIMEParams {
  import spark.implicits._

  protected var backgroundData: Option[DataFrame] = None

  def setBackgroundDataset(backgroundDataset: DataFrame): this.type = {
    this.backgroundData = Some(backgroundDataset)
    this
  }

  private def getSampleWeightUdf: UserDefinedFunction = {
    val kernelWidth = this.getKernelWidth

    val kernelFunc = (distance: Double) => {
      val t = distance / kernelWidth
      math.sqrt(math.exp(-t * t))
    }

    val weightUdf = UDFUtils.oldUdf(kernelFunc, DoubleType)
    weightUdf
  }

  override def explain(instances: Dataset[_]): DataFrame = {
    val regularization = this.getRegularization
    val df = instances.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("id", df)
    val weightCol = DatasetExtensions.findUnusedColumnName("weight", df)
    val distanceCol = DatasetExtensions.findUnusedColumnName("distance", df)

    val dfWithId = df.withColumn(idCol, monotonically_increasing_id()).cache

    val featureStats = createFeatureStats(this.backgroundData.getOrElse(dfWithId))

    val samples = createSamples(dfWithId, featureStats, idCol, distanceCol)
      .withColumn(weightCol, getSampleWeightUdf(col(distanceCol)))

    val transformed = getModel.transform(samples)

    val explainTargetCol = DatasetExtensions.findUnusedColumnName("target", transformed)

    val modelOutput = transformed.withColumn(explainTargetCol, this.getExplainTarget(transformed.schema))

    val fitted = modelOutput.groupByKey(row => row.getAs[Long](idCol)).mapGroups {
      case (id: Long, rows: Iterator[Row]) =>
        val (inputs, outputs, weights) = rows.map {
          row =>
            val input = row2Vector(row)
            val output = row.getAs[Double](explainTargetCol)
            val weight = row.getAs[Double](weightCol)
            (input, output, weight)
        }.toSeq.unzip3

        val inputsBV = BDM(inputs: _*)
        val outputsBV = BDV(outputs: _*)
        val weightsBV = BDV(weights: _*)
        val lassoResults = new LassoRegression(regularization).fit(inputsBV, outputsBV, weightsBV, fitIntercept = true)

        (id, lassoResults.coefficients.toArray, lassoResults.rSquared)
    }.toDF(idCol, this.getOutputCol, this.getMetricsCol)

    dfWithId.join(fitted, Seq(idCol), "inner").drop(idCol)
  }

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)

  protected def createSamples(df: DataFrame,
                              featureStats: Seq[FeatureStats],
                              idCol: String,
                              distanceCol: String): DataFrame

  protected def createFeatureStats(df: DataFrame): Seq[FeatureStats]

  protected def row2Vector(row: Row): BDV[Double]

  protected def validateInputSchema(schema: StructType): Unit = {
    require(
      !schema.fieldNames.contains(getMetricsCol),
      s"Input schema (${schema.simpleString}) already contains metrics column $getMetricsCol"
    )

    require(
      !schema.fieldNames.contains(getOutputCol),
      s"Input schema (${schema.simpleString}) already contains output column $getOutputCol"
    )
  }
}

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

class VectorLIME(override val uid: String)
  extends LIMEBase(uid) with HasInputCol {
  def this() = {
    this(Identifiable.randomUID("vec_lime"))
  }

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  override protected def createSamples(df: DataFrame,
                                       featureStats: Seq[FeatureStats],
                                       idCol: String,
                                       distanceCol: String): DataFrame = {
    val numSamples = this.getNumSamples

    val sampler = new LIMEVectorSampler(featureStats)

    val sampleType = StructType(Array(StructField(
      getInputCol, SQLDataTypes.VectorType
    )))

    val returnDataType = ArrayType(
      StructType(Seq(
        StructField("sample", sampleType),
        StructField("distance", DoubleType)
      ))
    )

    val samplesUdf = UDFUtils.oldUdf(
      {
        vector: SV =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          (1 to numSamples).map(_ => sampler.sample(vector))
      },
      returnDataType
    )

    df.withColumn("samples", explode(samplesUdf(col(getInputCol))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.sample").alias(getInputCol)
      )
  }

  override protected def row2Vector(row: Row): BDV[Double] = {
    row.getAs[SV](getInputCol).toBreeze
  }

  override protected def createFeatureStats(df: DataFrame): Seq[FeatureStats] = {
    val Row(std: SV) = df
      .select(Summarizer.metrics("std").summary(col($(inputCol))).as("summary"))
      .select("summary.std")
      .first()

    std.toArray.zipWithIndex.map {
      case (v, i) =>
        ContinuousFeatureStats(i, v)
    }
  }

  override protected def validateInputSchema(schema: StructType): Unit = {
    super.validateInputSchema(schema)
    require(
      schema(getInputCol).dataType == SQLDataTypes.VectorType,
      s"Field $getInputCol is expected to be vector type, but got ${schema(getInputCol).dataType} instead."
    )
  }
}