// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.{DataTypeParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

object FormOntologyLearner extends DefaultParamsReadable[FormOntologyLearner] {
  private[ml] def combineDataTypes(dt1: DataType, dt2: DataType): DataType = {
    (dt1, dt2) match {
      case (dt1, dt2) if DataType.equalsStructurally(dt1, dt2) => dt1
      case (StringType, DoubleType) => StringType
      case (ArrayType(et1, _), ArrayType(et2, _)) =>
        ArrayType(combineDataTypes(et1, et2))
      case (st1: StructType, st2: StructType) =>
        val fields1 = st1.fieldNames.toSet
        val fields2 = st2.fieldNames.toSet
        val sharedFields = fields1.intersect(fields2)
        val only1 = fields1.diff(sharedFields).toArray.sorted
        val only2 = fields2.diff(sharedFields).toArray.sorted
        StructType(
          sharedFields.toArray.sorted.map(fn =>
            StructField(fn, combineDataTypes(st1(fn).dataType, st2(fn).dataType))) ++
            only1.map(st1.apply) ++
            only2.map(st2.apply)
        )
      case _ =>
        throw new IllegalArgumentException(s"Cannot merge types $dt1 and $dt2 in the ontology")
    }
  }
}

class FormOntologyLearner(override val uid: String) extends Estimator[FormOntologyTransformer]
  with BasicLogging with DefaultParamsWritable with HasInputCol with HasOutputCol with Wrappable {
  logClass()

  def this() = this(Identifiable.randomUID("FormOntologyLearner"))

  private[ml] def extractOntology(fromRow: Row => AnalyzeResponse)(r: Row): StructType = {
    val fieldResults = fromRow(r.getStruct(0)).analyzeResult.documentResults.get.head.fields
    new StructType(fieldResults
      .mapValues(_.toFieldResultRecursive.toSimplifiedDataType)
      .map({ case (name, dt) => StructField(name, dt) }).toArray)
  }

  override def fit(dataset: Dataset[_]): FormOntologyTransformer = {
    val fromRow = AnalyzeResponse.makeFromRowConverter

    def combine(st1: StructType, st2: StructType): StructType = {
      FormOntologyLearner.combineDataTypes(st1, st2).asInstanceOf[StructType]
    }

    val mergedSchema = dataset.toDF()
      .select(col(getInputCol))
      .map(extractOntology(fromRow))(Encoders.kryo[StructType])
      .reduce(combine _)

    new FormOntologyTransformer()
      .setInputCol(getInputCol)
      .setOutputCol(getOutputCol)
      .setOntology(mergedSchema)
  }

  override def copy(extra: ParamMap): Estimator[FormOntologyTransformer] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, StructType(Seq()))
  }
}

object FormOntologyTransformer extends ComplexParamsReadable[FormOntologyTransformer]

class FormOntologyTransformer(override val uid: String) extends Model[FormOntologyTransformer]
  with BasicLogging with ComplexParamsWritable with HasInputCol with HasOutputCol with Wrappable {
  logClass()

  val ontology: DataTypeParam = new DataTypeParam(
    parent = this,
    name = "ontology",
    doc = "The ontology to cast values to",
    isValid = {
      case _: StructType => true
      case _ => false
    }
  )

  def getOntology: StructType = $(ontology).asInstanceOf[StructType]

  def setOntology(value: StructType): this.type = set(ontology, value)

  def this() = this(Identifiable.randomUID("FormOntologyTransformer"))

  override def copy(extra: ParamMap): FormOntologyTransformer = defaultCopy(extra)

  private[ml] def convertToOntology(fromRow: Row => AnalyzeResponse)(r: Row): Row = {
    val fieldResults = fromRow(r).analyzeResult.documentResults.get.head.fields
    Row.fromSeq(getOntology.map(sf =>
      fieldResults.get(sf.name).map(_.toFieldResultRecursive.viewAsDataType(sf.dataType))
    ))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val fromRow = AnalyzeResponse.makeFromRowConverter
    val convertToOntologyUDF = UDFUtils.oldUdf(convertToOntology(fromRow) _, getOntology)

    dataset.toDF()
      .withColumn(getOutputCol, convertToOntologyUDF(col(getInputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, getOntology)
  }
}
