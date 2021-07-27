// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.stages

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasInputCols, HasOutputCol}
import com.microsoft.ml.spark.core.serialize.ComplexParam
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.ml.param.{ParamMap, UDFParam, UDPyFParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.col

object UDFTransformer extends ComplexParamsReadable[UDFTransformer]

/** <code>UDFTransformer</code> takes as input input column, output column, and a UserDefinedFunction
  * returns a dataframe comprised of the original columns with the output column as the result of the
  * udf applied to the input column
  */
class UDFTransformer(val uid: String) extends Transformer with Wrappable with ComplexParamsWritable
  with HasInputCol with HasInputCols with HasOutputCol with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("UDFTransformer"))

  override protected lazy val pyInternalWrapper = true

  val udfScalaKey = "udfScala"
  val udfPythonKey = "udf"
  val invalidColumnSetError = s"If using multiple columns, only use set inputCols"

  setDefault(outputCol -> (this.uid + "_output"))

  override def setInputCol(value: String): this.type = {
    require(!isSet(inputCols), invalidColumnSetError)
    set(inputCol, value)
  }

  override def setInputCols(value: Array[String]): this.type = {
    require(!isSet(inputCol), invalidColumnSetError)
    set(inputCols, value)
  }

  val udfScala = new UDFParam(this, udfScalaKey, "User Defined Function to be applied to the DF input col")
  val udfPython = new UDPyFParam(this, udfPythonKey,
    "User Defined Python Function to be applied to the DF input col")
  val udfParams: Seq[ComplexParam[_]] = Seq(udfScala, udfPython)

  /** @group getParam */
  def getUDF: UserDefinedFunction = $(udfScala)

  /** @group getParam */
  def getUDPyF: UserDefinedPythonFunction = $(udfPython)

  /** @group setParam */
  def setUDF(value: UserDefinedFunction): this.type = {
    udfParams.filter(isSet).foreach(clear)
    set(udfScalaKey, value)
  }

  /** @group setParam */
  def setUDF(value: UserDefinedPythonFunction): this.type = {
    udfParams.filter(isSet).foreach(clear)
    set(udfPythonKey, value)
  }

  def applyUDF(col: Column): Column =  {
    if (isSet(udfScala)) getUDF(col)
    else getUDPyF(col)
  }

  def applyUDFOnCols(cols: Column*): Column =  {
    if (isSet(udfScala)) getUDF(cols: _*)
    else getUDPyF(cols: _*)
  }

  def getDataType: DataType =  {
    if (isSet(udfScala)) UDFUtils.unpackUdf(getUDF)._2
    else getUDPyF.dataType
  }

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from applying the udf to the inputted dataset
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(dataset.schema, logging = true)
      if (isSet(inputCol)) {
        dataset.withColumn(getOutputCol, applyUDF(dataset.col(getInputCol)))
      } else {
        dataset.withColumn(getOutputCol, applyUDFOnCols(getInputCols.map(col): _*))
      }
    })
  }

  def validateAndTransformSchema(schema: StructType): StructType = {
    if (isSet(inputCol)) schema(getInputCol) else schema(Set(getInputCols: _*))
    schema.add(StructField(getOutputCol, getDataType))
  }

  def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  def copy(extra: ParamMap): UDFTransformer = defaultCopy(extra)

}

