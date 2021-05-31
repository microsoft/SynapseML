package com.microsoft.ml.spark.explainers

import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame

class TabularSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCols
    with HasBackgroundData {

  def this() = {
    this(Identifiable.randomUID("TabularSHAP"))
  }

  def setInputCols(values: Array[String]): this.type = this.set(inputCols, values)

  override protected def createSamples(df: DataFrame, idCol: String, coalitionCol: String): DataFrame = {
    ???
  }
}
