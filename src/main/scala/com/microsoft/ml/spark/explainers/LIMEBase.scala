package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{stddev, _}
import org.apache.spark.sql.types.{ArrayType, DoubleType, NumericType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait LIMEParams extends HasNumSamples {
  self: LocalExplainer =>

  val regularization = new DoubleParam(
    this,
    "regularization",
    "Regularization param for the lasso. Default value: 0.",
    ParamValidators.gt(0)
  )

  val kernelWidth = new DoubleParam(
    this,
    "kernelWidth",
    "Kernel width. Default value: sqrt (number of features) * 0.75",
    ParamValidators.gt(0)
  )

  def getRegularization: Double = $(regularization)

  def setRegularization(v: Double): this.type = set(regularization, v)

  def getKernelWidth: Option[Double] = this.get(kernelWidth)

  def setKernelWidth(v: Double): this.type = set(kernelWidth, v)

  setDefault(numSamples -> 1000, regularization -> 0.0)
}

abstract class LIMEBase(override val uid: String) extends LocalExplainer with LIMEParams {
  import spark.implicits._

  protected var backgroundData: Option[DataFrame] = None

  def setBackgroundDataset(backgroundDataset: DataFrame): this.type = {
    this.backgroundData = Some(backgroundDataset)
    this
  }

  override def explain(instances: Dataset[_]): DataFrame = {
    val regularization = this.getRegularization
    val df = instances.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("id", df)
    val weightCol = DatasetExtensions.findUnusedColumnName("weight", df)
    val distanceCol = DatasetExtensions.findUnusedColumnName("distance", df)

    val dfWithId = df.withColumn(idCol, monotonically_increasing_id()).cache

    val featureStats = createFeatureStats(this.backgroundData.getOrElse(dfWithId))

    val kernelWidth = this.getKernelWidth.getOrElse(0.75 * math.sqrt(featureStats.size))

    val kernelFunc = (distance: Double) => {
      val t = distance / kernelWidth
      math.sqrt(math.exp(-t * t))
    }

    val weightUdf = UDFUtils.oldUdf(kernelFunc, DoubleType)

    val samples = createSamples(dfWithId, featureStats, idCol, distanceCol)
      .withColumn(weightCol, weightUdf(col(distanceCol)))

    val transformed = getModel.transform(samples)

    val explainTargetCol = DatasetExtensions.findUnusedColumnName("target", transformed)

    val modelOutput = transformed.withColumn(explainTargetCol, this.getExplainTarget(transformed.schema))

    val fitted = modelOutput.groupByKey(row => row.getAs[Long](idCol)).mapGroups {
      case (id: Long, rows: Iterator[Row]) =>
        val data = BDM(rows.map(row2Vector).toSeq: _*)
        val outputs = BDV(rows.map(row => row.getAs[Double](explainTargetCol)).toSeq: _*)
        val weights = BDV(rows.map(row => row.getAs[Double](weightCol)).toSeq: _*)
        val lassoResults = new LassoRegression(regularization).fit(data, outputs, weights, fitIntercept = false)

        // TODO: also expose R-squared for reporting fitting metrics
        (id, lassoResults.coefficients.toArray)
    }.toDF(idCol, this.getOutputCol)

    dfWithId.join(fitted, Seq(idCol), "inner").drop(idCol)
  }

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)

  protected def createSamples(df: DataFrame,
                              featureStats: Seq[FeatureStats],
                              idCol: String,
                              distanceCol: String): DataFrame

  protected def createFeatureStats(df: DataFrame): Seq[FeatureStats]

  protected def row2Vector(row: Row): BDV[Double]

  protected def validateInputSchema(schema: StructType): Unit
}

class TabularLIME(override val uid: String)
  extends LIMEBase(uid)
    with HasInputCols {

  def this() = {
    this(Identifiable.randomUID("tablime"))
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

    val rowType = StructType(featureStats.map(f => StructField(f.name, f.dataType)))

    val returnDataType = ArrayType(
      StructType(Seq(
        StructField("sample", rowType),
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

    df.withColumn("samples", explode(samplesUdf(df.columns.map(col): _*)))
      .select(col(idCol), col("samples.distance").alias(distanceCol), col("samples.sample.*"))
  }

  override protected def row2Vector(row: Row): BDV[Double] = {
    BDV(this.getInputCols.map(row.getAsDouble))
  }

  import spark.implicits._

  override protected def createFeatureStats(df: DataFrame): Seq[FeatureStats] = {
    val catFeatures = this.getInputCols.filter(this.getCategoricalFeatures.contains)
    val numFeatures = this.getInputCols.filterNot(this.getCategoricalFeatures.contains)

    val catFeatureStats = catFeatures.par.map {
      f =>
        val freqMap = df.select(col(f).cast(DoubleType).alias(f))
          .groupBy(f)
          .agg(count("*").alias("count").cast(DoubleType))
          .as[(Double, Double)]
          .collect()
          .toMap

        DiscreteFeatureStats(freqMap, df.schema(f).dataType, f)
    }

    val numAggs = numFeatures.map(f => stddev(f).cast(DoubleType).alias(f))

    val numFeatureStats = if (numAggs.nonEmpty) {
      val row = df.agg(numAggs.head, numAggs.tail: _*).head

      numFeatures.map {
        f =>
          val stddev = row.getAs[Double](f)
          ContinuousFeatureStats(stddev, df.schema(f).dataType, f)
      }.toSeq
    } else {
      Seq.empty
    }

    catFeatureStats.toArray ++: numFeatureStats
  }

  override protected def validateInputSchema(schema: StructType): Unit = {
    this.getInputCols.foreach {
      inputCol =>
        val field = schema.find(p => p.name == inputCol).getOrElse(
          throw new Exception(s"Field $inputCol not found in input schema: ${schema.simpleString}")
        )

        require(
          field.dataType.isInstanceOf[NumericType],
          s"Field $inputCol is expected to be a numeric type, but got ${field.dataType} instead."
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
    this(Identifiable.randomUID("veclime"))
  }

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  override protected def createSamples(df: DataFrame,
                                       featureStats: Seq[FeatureStats],
                                       idCol: String,
                                       distanceCol: String): DataFrame = {
    ???
  }

  override protected def row2Vector(row: Row): BDV[Double] = ???

  override protected def createFeatureStats(df: DataFrame): Seq[FeatureStats] = ???

  override protected def validateInputSchema(schema: StructType): Unit = ???
}