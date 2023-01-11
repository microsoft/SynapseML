// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.{StringStringMapParam}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.{ComplexParamsWritable, Transformer}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, functions => F, types => T}

class VowpalWabbitDSJsonTransformer(override val uid: String)
  extends Transformer
    with SynapseMLLogging
    with Wrappable
    with ComplexParamsWritable {
  import VowpalWabbitDSJsonTransformer._

  logClass()

  def this() = this(Identifiable.randomUID("VowpalWabbitDSJsonTransformer"))

  val dsJsonColumn = new Param[String](
    this,"dsJsonColumn", "Column containing ds-json. defaults to \"value\".")

  def getDsJsonColumn: String = $(dsJsonColumn)
  def setDsJsonColumn(value: String): this.type = set(dsJsonColumn, value)

  val rewards = new StringStringMapParam(
    this, "rewards", "Extract bandit reward(s) from DS json. Defaults to _label_cost.")

  def getRewards: Map[String, String] = $(rewards)
  def setRewards(v: Map[String, String]): this.type = set(rewards, v)

  setDefault(dsJsonColumn -> "value",
    rewards -> Map("reward" -> "_label_cost"))

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  private def eventIdField: T.StructField =
    T.StructField(EventIdColName, T.StringType, false)

  private def rewardFields: Seq[T.StructField] =
    getRewards.map { case (_, v) => T.StructField(v, T.FloatType, false) }.toSeq

  private def jsonSchema: T.StructType =
    T.StructType(Seq(
      eventIdField,
      T.StructField("_label_probability", T.FloatType, false),
      T.StructField("_labelIndex", T.IntegerType, false)) ++
      // extract rewards from JSON
      rewardFields)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      // TODO: extract all headers as well
      val jsonCol = F.col(JsonColName)

      val rewardCols = getRewards.map({ case (alias, col) => F.col(JsonColName).getField(col).alias(alias) }).toSeq

      val outputFields =
        dataset.schema.names.map(F.col) ++
          Seq(jsonCol,
            jsonCol.getField(EventIdColName).as(EventIdColName),
            F.struct(rewardCols: _*).as(RewardsColName),
            jsonCol.getField(LabelProbability).as(ProbabilityLoggedColName),
            jsonCol.getField(LabelIndex).as(ChosenActionIndexColName)
          )

      dataset.toDF
        .withColumn(JsonColName, F.from_json(F.col(getDsJsonColumn), jsonSchema))
        .select(outputFields: _ *)
    })
  }

  override def transformSchema(schema: StructType): StructType =
    T.StructType(schema.fields ++ Seq(
      T.StructField(JsonColName, jsonSchema, false),
      eventIdField,
      T.StructField(RewardsColName, T.StructType(rewardFields), false),
      T.StructField(ProbabilityLoggedColName, T.FloatType, false),
      T.StructField(ChosenActionIndexColName, T.IntegerType, false))
    )
}

object VowpalWabbitDSJsonTransformer {
  val EventIdColName = "EventId"

  val JsonColName = "json"

  val LabelProbability = "_label_probability"

  val LabelIndex = "_labelIndex"

  val ProbabilityLoggedColName = "probLog"

  val ProbabilityPredictedColName = "probPred"

  val ChosenActionIndexColName = "chosenActionIndex"

  val RewardsColName = "rewards"

  val HeaderColNames = Seq(EventIdColName, RewardsColName, ProbabilityLoggedColName, ChosenActionIndexColName)
}
