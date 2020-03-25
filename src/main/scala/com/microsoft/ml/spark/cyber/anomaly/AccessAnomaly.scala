// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cyber.anomaly

import org.apache.spark.ml.param.{BooleanParam, DoubleParam, IntParam, Param, ParamMap, Params, TransformerParam}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model, NamespaceInjections, Pipeline, PipelineModel, PipelineStage, Transformer}
import com.microsoft.ml.spark.core.contracts.{HasOutputCol, Wrappable}
import com.microsoft.ml.spark.cyber.feature.{PartitionedMinMaxScaler, PartitionedStandardScaler, PartitionedStringIndexer}
import com.microsoft.ml.spark.io.DataFrameExtensions
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DoubleType, StructType}
import com.microsoft.ml.spark.core.schema.DatasetExtensions.findUnusedColumnName
import org.apache.spark.ml.feature.{SQLTransformer, StandardScaler, StringIndexer}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}

object AccessAnomaly extends DefaultParamsReadable[AccessAnomaly]

trait AccessAnomalyParams extends Params with HasOutputCol {
  val tenantCol = new Param[String](this,
    "tenantCol",
    "The name of the tenant column. " +
      "This is a unique identifier used to partition the dataframe into independent " +
      "groups where the values in each such group are completely isolated from one another. " +
      "Note: if this column is irrelevant for your data, " +
      "then just create a tenant column and give it a single value for all rows."
  )
  setDefault(tenantCol -> "tenant")

  def getTenantCol: String = $(tenantCol)

  def setTenantCol(v: String): this.type = set(tenantCol, v)

  val userCol = new Param[String](this,
    "userCol",
    "The name of the user column. " +
      "This is a the name of the user column in the dataframe."
  )
  setDefault(userCol -> "user")

  def getUserCol: String = $(userCol)

  def setUserCol(v: String): this.type = set(userCol, v)

  val resCol = new Param[String](this,
    "resCol",
    "The name of the resource column. " +
      "This is a the name of the resource column in the dataframe."
  )
  setDefault(resCol -> "res")

  def getResCol: String = $(resCol)

  def setResCol(v: String): this.type = set(resCol, v)

  val accessCol = new Param[String](this,
    "accessCol",
    "The name times a given user accessed a resource " +
      "(usually based on access counts per time unit). "
  )
  setDefault(accessCol -> "rating")

  def getRatingCol: String = $(accessCol)

  def setRatingCol(v: String): this.type = set(accessCol, v)

  val normalizeRating = new BooleanParam(this,
    "normalizeRating",
    "whether to use normalize the output rating to mean 0 std 1"
  )
  setDefault(normalizeRating -> true)

  def getNormalizeRating: Boolean = $(normalizeRating)

  def setNormalizeRating(v: Boolean): this.type = set(normalizeRating, v)

  val rank = new IntParam(this,
    "rank",
    "rank is the number of latent factors in the model (defaults to 10)."
  )
  setDefault(rank -> 10)

  def getRank: Int = $(rank)

  def setRank(v: Int): this.type = set(rank, v)

  val maxIter = new IntParam(this,
    "maxIter",
    "maxIter is the maximum number of iterations to run (defaults to 25)."
  )
  setDefault(maxIter -> 25)

  def getMaxIter: Int = $(maxIter)

  def setMaxIter(v: Int): this.type = set(maxIter, v)

  val reg = new DoubleParam(this,
    "reg",
    "reg specifies the regularization parameter in ALS (defaults to 0.1)."
  )
  setDefault(reg -> 1.0)

  def getReg: Double = $(reg)

  def setReg(v: Double): this.type = set(reg, v)

  val numBlocks = new IntParam(this,
    "numBlocks",
    "numBlocks is the number of blocks the users and items will be partitioned into " +
      "in order to parallelize computation " +
      "(defaults to |tenants| if separate_tenants is False else 10)."
  )
  setDefault(numBlocks -> 10)

  def getNumBlocks: Int = $(numBlocks)

  def setNumBlocks(v: Int): this.type = set(numBlocks, v)

  val separateTenants = new BooleanParam(this,
    "separateTenants",
    "separateTenants applies the algorithm per tenant in isolation. " +
      "Setting to True may reduce runtime significantly, if number of tenant is large, " +
      "but will increase accuracy. (defaults to False)."
  )
  setDefault(separateTenants -> false)

  def getSeprateTetents: Boolean = $(separateTenants)

  def setSeparateTenants(v: Boolean): this.type = set(separateTenants, v)

  val implicitCF = new BooleanParam(this,
    "implicitCF",
    "'implicit' specifies whether to use the explicit feedback ALS variant or one adapted " +
      "for implicit feedback data (defaults to false which means using explicit feedback). "
  )
  setDefault(implicitCF -> true)

  def getImplicitCF: Boolean = $(implicitCF)

  def setImplicitCF(v: Boolean): this.type = set(implicitCF, v)

  val nonnegative = new BooleanParam(this,
    "nonnegative",
    "whether to use nonnegative ALS"
  )
  setDefault(nonnegative -> true)

  def getNonnegative: Boolean = $(nonnegative)

  def setNonegative(v: Boolean): this.type = set(nonnegative, v)

  val coldStartStrategy = new Param[String](this,
    "coldStartStrategy",
    "cold start strategy for internal ALS "
  )
  setDefault(coldStartStrategy -> "drop")

  def getColdStartStrategy: String = $(coldStartStrategy)

  def setColdStartStrategy(v: String): this.type = set(coldStartStrategy, v)

  val alpha = new DoubleParam(this,
    "alpha",
    "alpha' is a parameter applicable to the implicit feedback variant of ALS that governs " +
      "the baseline confidence in preference observations (defaults to 1.0). "
  )
  setDefault(alpha -> 1.0)

  def getAlpha: Double = $(alpha)

  def setAlpha(v: Double): this.type = set(alpha, v)

  val negSamplingFraction = new DoubleParam(this,
    "negSamplingFraction",
    "negSamplingFraction is a parameter applicable to the explicit feedback" +
      " variant of ALS that governs that is used to generate a sample from the complement set of " +
      "(user, res) access patterns seen in the training data. " +
      "For example, a value of 2 indicates that the complement set should be an " +
      "order of twice the size of the distinct (user, res) pairs in the training. (defaults to 2)."
  )
  setDefault(negSamplingFraction -> 2.0)

  def getNegSamplingFraction: Double = $(negSamplingFraction)

  def setNegSamplingFraction(v: Double): this.type = set(negSamplingFraction, v)

  val negSamplingScore = new DoubleParam(this,
    "negSamplingScore",
    "'negSamplingScore' is a parameter applicable to the explicit feedback variant of ALS that governs " +
      "the value to assign to the values of the complement set. (defaults to 1.0)."
  )
  setDefault(negSamplingScore -> 1.0)

  def getNegSamplingScore: Double = $(negSamplingScore)

  def setNegSamplingScore(v: Double): this.type = set(negSamplingScore, v)

}

class AccessAnomaly(override val uid: String) extends Estimator[AccessAnomalyModel]
  with DefaultParamsWritable with Wrappable with AccessAnomalyParams {

  def this() = this(Identifiable.randomUID("AccessAnomaly"))

  override def copy(extra: ParamMap): AccessAnomaly = defaultCopy(extra)

  private def getNegativeSamplers(indexedUserCol: String,
                                  indexedResCol: String): Seq[PipelineStage] = {
    if (getImplicitCF) {
      Seq()
    } else {
      Seq(
        new ComplementSampler()
          .setPartitionKey(getTenantCol)
          .setInputCols(Array(indexedUserCol, indexedResCol))
          .setSamplingFactor(getNegSamplingFraction),
        new SQLTransformer()
          .setStatement(s"SELECT *, lit($getNegSamplingScore) as $getRatingCol")
      )
    }
  }

  private def getAls = {
    new ALS()
      .setRank(getRank)
      .setMaxIter(getMaxIter)
      .setRegParam(getReg)
      .setNumItemBlocks(getNumBlocks)
      .setNumUserBlocks(getNumBlocks)
      .setImplicitPrefs(getImplicitCF)
      .setNonnegative(getNonnegative)
      .setColdStartStrategy(getColdStartStrategy)
      .setAlpha(getAlpha)
  }

  private def getPreprocessor(schema: StructType): Pipeline = {
    val indexedUserCol = findUnusedColumnName("indexedUser", schema)
    val indexedResCol = findUnusedColumnName("indexedRes", schema)

    val indexers = Seq( //TODO uniqueify Indexer
      new StringIndexer()
        .setInputCol(getUserCol)
        .setOutputCol(indexedUserCol),
      new StringIndexer()
        .setInputCol(getResCol)
        .setOutputCol(indexedResCol)
    )

    val negativeSamplers = getNegativeSamplers(indexedUserCol, indexedResCol)

    new Pipeline().setStages((indexers ++ negativeSamplers).toArray)
  }

  override def fit(data: Dataset[_]): AccessAnomalyModel = {
    val df = data.toDF()
    val indexedUserCol = findUnusedColumnName("indexedUser", df)
    val indexedResCol = findUnusedColumnName("indexedRes", df)

    val fitPreprocessor = getPreprocessor(df.schema).fit(df)
    val preprocessedDF = fitPreprocessor.transform(df).cache()

    val als = getAls.setUserCol(indexedUserCol)
      .setItemCol(indexedResCol)
      .setRatingCol(getRatingCol)

    val alsModel = if (getSeprateTetents) {
      val tenants = preprocessedDF
        .select(getTenantCol).distinct()
        .orderBy(getTenantCol).collect()
        .map(r => r.getString(0))
      val (userFactorDfs, itemFactorDfs) = tenants.map { tenant =>
        val inputDF = preprocessedDF.filter(col(getTenantCol) === lit(tenant)).cache()
        val model = als.fit(inputDF)
        inputDF.unpersist()
        (model.userFactors, model.itemFactors)
      }.unzip
      val userFactors = userFactorDfs.reduce(_ union _)
      val itemFactors = itemFactorDfs.reduce(_ union _)
      NamespaceInjections.alsModel(als.uid, als.getRank, userFactors, itemFactors)
        .setUserCol(als.getUserCol)
        .setItemCol(als.getItemCol)
        .setColdStartStrategy(als.getColdStartStrategy)
        .setPredictionCol(als.getPredictionCol)
    } else {
      als.fit(preprocessedDF)
    }

    val comparison = SQLTransformer()
    val postprocessor = if (getNormalizeRating) {
      Seq(new PartitionedStandardScaler()
        .setInputCol(getRatingCol)
        .setOutputCol(getRatingCol)
        .setPartitionKey(getTenantCol))
    } else {
      Seq()
    }

    val pipe = new Pipeline().setStages(Array(fitPreprocessor, alsModel) ++ postprocessor).fit(df)
    new AccessAnomalyModel(uid).setPipeline(pipe)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getRatingCol, DoubleType)
  }
}

object AccessAnomalyModel extends ComplexParamsReadable[AccessAnomalyModel]

class AccessAnomalyModel(override val uid: String) extends Model[AccessAnomalyModel]
  with ComplexParamsWritable {

  val accessModel = new TransformerParam(this, "accessModel",
    "the model that predicts the (normalized) number of acceses")

  def getPipeline: Transformer = $(accessModel)

  def setPipeline(v: Transformer): this.type = set(accessModel, v)

  def this() = this(Identifiable.randomUID("AccessAnomalyModel"))

  override def copy(extra: ParamMap): AccessAnomalyModel = defaultCopy(extra)

  override def transform(data: Dataset[_]): DataFrame = {
    getPipeline.transform(data)
  }

  override def transformSchema(schema: StructType): StructType = {
    getPipeline.transformSchema(schema)
  }

}
