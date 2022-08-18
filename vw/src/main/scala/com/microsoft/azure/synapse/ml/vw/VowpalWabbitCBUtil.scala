package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.{Column, DataFrame, functions => F, types => T}
import com.microsoft.azure.synapse.ml.policyeval.PolicyEvalUDAFUtil

object VowpalWabbitCBUtil {
  private val EventIdCol = "EventId"
//  private val DsJsonCol = "value"
  private val JsonCol = "json"
//  private val SplitIdCol = "splitId"
  private val ProbabilityLoggedCol = "probLog"
  private val ProbabilityPredictedCol = "probPred"
  private val ChosenActionIndexCol = "chosenActionIndex"
  private val RewardsCol = "rewards"
  private val MetricStratificationCol = "stratifications"

  private val JsonFields = Seq(
    T.StructField(EventIdCol, T.StringType, false),
    T.StructField("_label_cost", T.FloatType, false),
    T.StructField("_label_probability", T.FloatType, false),
    T.StructField("_labelIndex", T.IntegerType, false))

  // define reward independent metrics
  val GlobalMetrics: Seq[Column] = Seq(
    F.count("*").alias("exampleCount"),
    F.sum(F.when(F.expr(s"$ProbabilityPredictedCol > 0"), 1).otherwise(0))
      .alias("probPredNonZeroCount"),
    F.min("w").alias("minimum importance weights"),
    F.max("w").alias("maximum importance weights"),
    F.expr("avg(w)").alias("average of importance weights"),
    F.expr("avg(w * w)").alias("average of squared importance weights"),
    F.expr("max(w)/count(*)").alias("proportion of maximum importance weight"),
    F.expr("approx_percentile(w, array(0.25, 0.5, 0.75, 0.95))")
      .alias("quantiles (0.25, 0.5, 0.75, 0.95) of importance weight"))

  /**
    *
    * @param dfInput input data frame.
    * @param vwArgs command line arguments passed to VowpalWabbit.
    * @param dsjsonColumn column containing ds-json. defaults to "value".
    * @param jsonColumn optional column name containing the parsed ds-json.
    * @param splitCol column use to split the data into individual steps, after every step the models learned
    *                 for each partition are synchronized).
    * @param splitColValues optional pre-computed or subset of split values to process, if empty data will queried.
    * @return
    */
  def eval(dfInput: DataFrame,
           vwArgs: String,
           dsjsonColumn: String = "value",
           jsonColumn: Option[String] = None,
           splitCol: String = "splitId",
           splitColValues: Seq[String] = Seq.empty,
           minImportanceWeight: Float = 0,
           maxImportanceWeight: Float = 100 // TODO: is this a good default?
           ): DataFrame = {

    val df = dfInput
      .select(
        // extract JSON if it's not already there
        (if (jsonColumn.nonEmpty) F.col(jsonColumn.get)
        else F.from_json(F.col(dsjsonColumn), T.StructType(JsonFields)))
          .alias(JsonCol)
      )
      .withColumn(EventIdCol, F.col(s"$JsonCol.$EventIdCol"))

    // setup VW
    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs(vwArgs)
      .setInputCol(dsjsonColumn)
      .setSplitCol(splitCol)
      .setPredictionIdCol(EventIdCol)

    // if not supplied the data will queried (can be expensive)
    if (splitColValues.nonEmpty)
      vw.setSplitColValues(splitColValues.toArray)

    // fit the model
    val model = vw.fit(df)

    // fetch the predictions
    model.getOneStepAheadPredictions
  }

  /**
    * @param df
    * @param dsjsonColumn column containing ds-json. defaults to "value".
    * @param jsonColumn optional column name containing the parsed ds-json.
    * @param rewards list of rewards to be computed. defaults online observed label_cost.
    * @param metricStratification
    * @return
    */
  def extractHeaders(df: DataFrame,
                     dsJsonColumn: String = "value",
                     jsonColumn: Option[String] = None,
                     rewards: Map[String, String] = Map("reward" -> "json._label_cost"),
                     metricStratification: Map[String, String] = Map.empty,
                     additionalJsonFields: Seq[T.StructField] = Seq.empty): DataFrame = {

    assert (rewards.nonEmpty, "At least 1 reward needs to be specified")

    def mapToCols(m: Map[String, String]): Seq[Column] =
      m.map({ case (alias, col) => F.col(col).alias(alias) }).toSeq

    val outputs = Seq(
      F.col(s"$JsonCol.$EventIdCol").alias(EventIdCol),
      F.col("json._label_probability").alias(ProbabilityLoggedCol),
      F.col("json._labelIndex").alias(ChosenActionIndexCol),
      F.struct(mapToCols(rewards): _*).alias(RewardsCol))

    // conditionally add metric stratifications
    val outputsAll =
      if (metricStratification.isEmpty) outputs
      else outputs :+ F.struct(mapToCols(metricStratification): _*).alias(MetricStratificationCol)

    df
      .select(
        // extract JSON if it's not already there
        (if (jsonColumn.nonEmpty) F.col(jsonColumn.get)
        else F.from_json(F.col(dsJsonColumn), T.StructType(JsonFields ++ additionalJsonFields)))
          .alias(JsonCol)
      )
      .select(outputsAll: _*)
  }

  case class RewardColumn(name: String, col: String, idx: Int) {
    def minRewardCol: String = s"min_reward_$idx"
    def maxRewardCol: String = s"max_reward_$idx"
  }

  def rewardColumns(schema: T.StructType): Seq[RewardColumn] =
    schema(RewardsCol)
      .dataType.asInstanceOf[T.StructType]
      .fields
      .zipWithIndex
      .map({ case (rewardField, idx) =>
        RewardColumn(rewardField.name, s"$RewardsCol.${rewardField.name}", idx) })
      .toSeq

  def perRewardMetrics(rewardsCol: Seq[RewardColumn],
                       minImportanceWeight: Float = 0,
                       maxImportanceWeight: Float = 100): Seq[Column] =
    rewardsCol
      .map({ rewardCol => {
        F.struct(
          F.first(F.col(rewardCol.minRewardCol)).alias("minReward"),
          F.first(F.col(rewardCol.maxRewardCol)).alias("maxReward"),
          F.expr(s"snips($ProbabilityLoggedCol, ${rewardCol.col}, $ProbabilityPredictedCol, count)").alias("snips"),
          F.expr(s"ips($ProbabilityLoggedCol, ${rewardCol.col}, $ProbabilityPredictedCol, count)").alias("ips"),
          F.expr(s"cressieRead($ProbabilityLoggedCol, ${rewardCol.col}, $ProbabilityPredictedCol, count, " +
            s"$minImportanceWeight, $maxImportanceWeight)")
            .alias("cressieRead"),
          F.expr(s"cressieReadInterval($ProbabilityLoggedCol, ${rewardCol.col}, $ProbabilityPredictedCol, count, " +
            s"$minImportanceWeight, $maxImportanceWeight, ${rewardCol.minRewardCol}, ${rewardCol.maxRewardCol})")
            .alias("cressieReadInterval"),
          F.expr(s"cressieReadIntervalEmpirical($ProbabilityLoggedCol, ${rewardCol.col}, " +
            s"$ProbabilityPredictedCol, count, " +
            s"$minImportanceWeight, $maxImportanceWeight, ${rewardCol.minRewardCol}, ${rewardCol.maxRewardCol})")
            .alias("cressieReadIntervalEmpirical")
        ).alias(rewardCol.name)
      }})

  // Continuous success evaluation/experimentation
  def metrics(headers: DataFrame,
              predictions: DataFrame,
              metricStratification: Seq[String] = Seq.empty,
              minImportanceWeight: Float = 0,
              maxImportanceWeight: Float = 100): DataFrame = {

    // fetch reward columns out of nested structured
    val rewardsCol = rewardColumns(headers.schema)

    // calculate min/max for each reward
    val minMaxes = rewardsCol
      .flatMap({ rewardCol =>
        Seq(F.min(rewardCol.col).alias(rewardCol.minRewardCol),
            F.max(rewardCol.col).alias(rewardCol.maxRewardCol))})

    val minMaxRewards = headers.agg(minMaxes.head, minMaxes.drop(1): _*)

    // register contextual bandit policy evaluation aggregates
    PolicyEvalUDAFUtil.registerUdafs()

    val metrics = GlobalMetrics ++ perRewardMetrics(rewardsCol, minImportanceWeight, maxImportanceWeight)

    //  join predictions with data to compute all the metrics
    headers
      .join(predictions, EventIdCol)
      .crossJoin(minMaxRewards.hint("broadcast"))
      // probability predicted = 1 if top action matches observed action, otherwise 0
      // Note: using the probability distribution produces by cb_adf_explore only increases variance
      // and doesn't help model selection. also the offline policy can't influence what data is getting
      // collected offline
      .withColumn(ProbabilityPredictedCol,
        F.when(F.expr(s"element_at(predictions, 1).action == $ChosenActionIndexCol"), 1f).otherwise(0f))
      // example weight, defaults to 1
      .withColumn("count", F.lit(1))
      .withColumn("w", F.col(ProbabilityPredictedCol) / F.col(ProbabilityLoggedCol))
      // optional stratification
      .groupBy(metricStratification.map({ c => F.col(s"$MetricStratificationCol.$c") }): _*)
      // TODO: really? is there some nicer way of doing this?
      .agg(metrics.head, metrics.drop(1): _*)
  }
}
