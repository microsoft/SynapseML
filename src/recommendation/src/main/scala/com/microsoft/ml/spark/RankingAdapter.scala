/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.ml.spark


import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.language.existentials

class RecommenderAdapter(override val uid: String)
  extends Estimator[RecommenderAdapterModel] with ComplexParamsWritable with RecommendationSplitFunctions {

  def this() = this(Identifiable.randomUID("RecommenderAdapter"))

  val recommender = new EstimatorParam(this, "recommender", "estimator for selection", {x: Estimator[_] => true})

  /** @group getParam */
  def getRecommender: Estimator[_ <: Model[_]] = $(recommender)

  /** @group setParam */
  def setRecommender(value: Estimator[_ <: Model[_]]): this.type = set(recommender, value)

  val mode: Param[String] = new Param(this, "mode", "recommendation mode")

  /** @group getParam */
  def getMode: String = $(mode)

  /** @group setParam */
  def setMode(value: String): this.type = set(mode, value)


  def transformSchema(schema: StructType): StructType = {
    val model = getRecommender.asInstanceOf[ALS]
    getMode match {
      case "allUsers" => new StructType()
        .add("userCol", IntegerType)
        .add("recommendations", ArrayType(
          new StructType().add("itemCol", IntegerType).add("rating", FloatType))
        )
      case "allItems" => new StructType()
        .add("itemCol", IntegerType)
        .add("recommendations", ArrayType(
          new StructType().add("userCol", IntegerType).add("rating", FloatType))
        )
      case "normal" => model.transformSchema(schema)
    }
  }

  def fit(dataset: Dataset[_]): RecommenderAdapterModel = {
    new RecommenderAdapterModel()
      .setRecommenderModel(getRecommender.fit(dataset))
      .setMode(getMode)
  }

  override def copy(extra: ParamMap): RecommenderAdapter = {
    defaultCopy(extra)
  }

}

object RecommenderAdapter extends ComplexParamsReadable[RecommenderAdapter]

/**
  * Model from train validation split.
  *
  * @param uid Id.
  */
class RecommenderAdapterModel private[ml](val uid: String)
  extends Model[RecommenderAdapterModel] with ComplexParamsWritable with Wrappable {

  def this() = this(Identifiable.randomUID("RecommenderAdapterModel"))

  val recommenderModel = new TransformerParam(this, "recommenderModel", "recommenderModel", { x: Transformer => true })

  def setRecommenderModel(m: Model[_]): this.type = set(recommenderModel, m)

  def getRecommenderModel: Model[_] = $(recommenderModel).asInstanceOf[Model[_]]

  val mode: Param[String] = new Param(this, "mode", "recommendation mode")

  /** @group getParam */
  def getMode: String = $(mode)

  /** @group setParam */
  def setMode(value: String): this.type = set(mode, value)

  val nItems: IntParam = new IntParam(this, "nItems", "recommendation mode")

  /** @group getParam */
  def getNItems: Int = $(nItems)

  /** @group setParam */
  def setNItems(value: Int): this.type = set(nItems, value)

  val nUsers: IntParam = new IntParam(this, "nUsers", "recommendation mode")

  /** @group getParam */
  def getNUsers: Int = $(nUsers)

  /** @group setParam */
  def setNUsers(value: Int): this.type = set(nUsers, value)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val model = getRecommenderModel.asInstanceOf[ALSModel]
    getMode match {
      case "allUsers" => model.recommendForAllUsers(getNItems)
      case "allItems" => model.recommendForAllItems(getNUsers)
      case "normal" => model.transform(dataset)
    }
  }

  def transformSchema(schema: StructType): StructType = {
    val model = getRecommenderModel.asInstanceOf[ALSModel]
    getMode match {
      case "allUsers" => new StructType()
        .add("userCol", IntegerType)
        .add("recommendations", ArrayType(
            new StructType().add("itemCol", IntegerType).add("rating", FloatType))
        )
      case "allItems" => new StructType()
        .add("itemCol", IntegerType)
        .add("recommendations", ArrayType(
          new StructType().add("userCol", IntegerType).add("rating", FloatType))
        )
      case "normal" => model.transformSchema(schema)
    }
  }

  override def copy(extra: ParamMap): RecommenderAdapterModel = {
    defaultCopy(extra)
  }

}

object RecommenderAdapterModel extends ComplexParamsReadable[RecommenderAdapterModel]