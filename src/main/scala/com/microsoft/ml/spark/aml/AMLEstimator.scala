// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol, Wrappable}
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

trait AMLParams extends Wrappable with DefaultParamsWritable {

  /** Tokenize the input when set to true
    * @group param
    */
  val useTokenizer = new BooleanParam(this, "useTokenizer", "Whether to tokenize the input")

  /** @group getParam */
  final def getUseTokenizer: Boolean = $(useTokenizer)

  /*** @group param*/
  val resource = new Param[String](this, "resource", "URL to login resource")

  /** @group getParam */
  final def getResource: String = $(resource)

  /*** @group param*/
  val clientId = new Param[String](this, "clientId", "Client ID of application")

  /** @group getParam */
  final def getClientId: String = $(clientId)

  /*** @group param*/
  val clientSecret = new Param[String](this, "clientSecret", "Client secret of application")

  /** @group getParam */
  final def getClientSecret: String = $(clientSecret)

  /*** @group param*/
  val tenantId = new Param[String](this, "tenantId", "App tenant ID")

  /** @group getParam */
  final def getTenantId: String = $(clientSecret)

  /*** @group param*/
  val subscriptionId = new Param[String](this, "subscriptionId", "App subscription ID")

  /** @group getParam */
  final def getSubscriptionId: String = $(subscriptionId)

  /*** @group param*/
  val region = new Param[String](this, "region", "User geographic region")

  /** @group getParam */
  final def getRegion: String = $(region)

  /*** @group param*/
  val resourceGroup = new Param[String](this, "resourceGroup", "App resource group")

  /** @group getParam */
  final def getResourceGroup: String = $(resourceGroup)

  /*** @group param*/
  val workspace = new Param[String](this, "workspace", "App workspace")

  /** @group getParam */
  final def getWorkspace: String = $(workspace)

  /*** @group param*/
  val experimentName = new Param[String](this, "experimentName", "Name of experiment")

  /** @group getParam */
  final def getExperimentName: String = $(experimentName)

  /*** @group param*/
  val runFilePath = new Param[String](this, "runFilePath",
    "Path to where definition JSON and project ZIP folder are located")

  /** @group getParam */
  final def getRunFilePath: String = $(runFilePath)

}

object AMLEstimator extends DefaultParamsReadable[AMLEstimator]

/** Featurize text.
  *
  * @param uid The id of the module
  */
class AMLEstimator(override val uid: String)
  extends Estimator[AMLModel]
    with AMLParams with HasInputCol with HasOutputCol {
  def this() = this(Identifiable.randomUID("AMLEstimator"))

  setDefault(outputCol, uid + "_output")

  def setUseTokenizer(value: Boolean): this.type = set(useTokenizer, value)

  setDefault(useTokenizer -> true)

  def setResource(value: String): this.type = set(resource, value)

  setDefault(resource -> "https://login.microsoftonline.com")

  def setClientId(value: String): this.type = set(clientId, value)

  def setClientSecret(value: String): this.type = set(clientSecret, value)

  def setTenantId(value: String): this.type = set(tenantId, value)

  def setSubscriptionId(value: String): this.type = set(subscriptionId, value)

  def setRegion(value: String): this.type = set(region, value)

  def setResourceGroup(value: String): this.type = set(resourceGroup, value)

  def setWorkspace(value: String): this.type = set(workspace, value)

  def setExperimentName(value: String): this.type = set(experimentName, value)

  def setRunFilePath(value: String): this.type = set(runFilePath, value)

  override def fit(dataset: Dataset[_]): AMLModel = {
    ???
  }

  override def copy(extra: ParamMap): this.type =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
   ???
  }

}

class AMLModel(val uid: String,
              fitPipeline: PipelineModel,
              colsToDrop: List[String])
  extends Model[AMLModel] with DefaultParamsWritable {

  override def copy(extra: ParamMap): AMLModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    ???
  }

  override def transformSchema(schema: StructType): StructType = {
    ???
  }
}

object AMLModel extends DefaultParamsReadable[AMLModel]
