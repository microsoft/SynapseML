// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import com.microsoft.ml.spark.schema._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import reflect.runtime.universe.TypeTag

import scala.math.Ordering
import scala.reflect.ClassTag

object ValueIndexer extends DefaultParamsReadable[ValueIndexer] {
  def validateAndTransformSchema(schema: StructType, outputCol: String): StructType = {
    val newField = NominalAttribute.defaultAttr.withName(outputCol).toStructField()
    if (schema.fieldNames.contains(outputCol)) {
      val index = schema.fieldIndex(outputCol)
      val fields = schema.fields
      fields(index) = newField
      StructType(fields)
    } else {
      schema.add(newField)
    }
  }
}

/**
  * Fits a dictionary of values from the input column.
  * Model then transforms a column to a categorical column of the given array of values.
  * Similar to StringIndexer except it can be used on any value types.
  */
class ValueIndexer(override val uid: String) extends Estimator[ValueIndexerModel]
  with HasInputCol with HasOutputCol with MMLParams {

  def this() = this(Identifiable.randomUID("ValueIndexer"))

  /**
    * Fits the dictionary of values from the input column.
    *
    * @param dataset The input dataset to train.
    * @return The model for transforming columns to categorical.
    */
  override def fit(dataset: Dataset[_]): ValueIndexerModel = {
    val dataType = dataset.schema(getInputCol).dataType
    val levels = dataset.select(getInputCol).distinct().collect().map(row => row(0))
    // Compute the levels
    new ValueIndexerModel(uid, levels, dataType, getInputCol, getOutputCol)
  }

  override def copy(extra: ParamMap): Estimator[ValueIndexerModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    ValueIndexer.validateAndTransformSchema(schema, getOutputCol)
}

/**
  * Model produced by [[ValueIndexer]].
  */
class ValueIndexerModel(val uid: String,
                        val levels: Array[_],
                        val dataType: DataType,
                        val inputCol: String,
                        val outputCol: String)
    extends Model[ValueIndexerModel] with MLWritable {

  override def write: MLWriter =
    new ValueIndexerModel.ValueIndexerModelWriter(uid, levels, dataType, inputCol, outputCol)

  override def copy(extra: ParamMap): ValueIndexerModel =
    new ValueIndexerModel(uid, levels, dataType, inputCol, outputCol)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Transform the input column to categorical
    dataType match {
      case _: IntegerType => addCategoricalColumn[Int](dataset)(Ordering[Int])
      case _: LongType => addCategoricalColumn[Long](dataset)(Ordering[Long])
      case _: DoubleType => addCategoricalColumn[Double](dataset)(Ordering[Double])
      case _: StringType => addCategoricalColumn[String](dataset)(Ordering[String])
      case _: BooleanType => addCategoricalColumn[Boolean](dataset)(Ordering[Boolean])
      case _ => throw new Exception("Unsupported Categorical type " + dataType.toString)
    }
  }

  private def addCategoricalColumn[T: TypeTag](dataset: Dataset[_])
                                     (ordering: Ordering[T])
                                     (implicit ct: ClassTag[T]): DataFrame = {
    val castLevels = levels.map(_.asInstanceOf[T])
    val map = new CategoricalMap(castLevels.sorted(ordering))
    val getIndex = udf((level: T) => map.getIndex(level))
    // Add the MML style and MLLIB style metadata for categoricals
    val metadata = map.toMetadata(map.toMetadata(dataset.schema(inputCol).metadata, true), false)
    val inputColIndex = getIndex(dataset(inputCol))
    dataset.withColumn(outputCol, inputColIndex.as(outputCol, metadata))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    ValueIndexer.validateAndTransformSchema(schema, outputCol)
}

object ValueIndexerModel extends MLReadable[ValueIndexerModel] {

  private val levelsPart = "levels"
  private val dataPart = "data"

  override def read: MLReader[ValueIndexerModel] = new ValueIndexerModelReader

  override def load(path: String): ValueIndexerModel = super.load(path)

  /** [[MLWriter]] instance for [[ValueIndexerModel]] */
  private[ValueIndexerModel]
  class ValueIndexerModelWriter(val uid: String,
                                val levels: Array[_],
                                val dataType: DataType,
                                val inputCol: String,
                                val outputCol: String)
    extends MLWriter {

    private case class Data(uid: String, dataType: DataType, inputCol: String, outputCol: String)

    override protected def saveImpl(path: String): Unit = {
      val overwrite = this.shouldOverwrite
      val qualPath = PipelineUtilities.makeQualifiedPath(sc, path)
      // Required in order to allow this to be part of an ML pipeline
      PipelineUtilities.saveMetadata(uid,
        ValueIndexerModel.getClass.getName.replace("$", ""),
        new Path(path, "metadata").toString,
        sc,
        overwrite)

      // save the levels
      ObjectUtilities.writeObject(levels, qualPath, levelsPart, sc, overwrite)

      // save model data
      val data = Data(uid, dataType, inputCol, outputCol)
      val dataPath = new Path(qualPath, dataPart).toString
      val saveMode =
        if (overwrite) SaveMode.Overwrite
        else SaveMode.ErrorIfExists
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.mode(saveMode).parquet(dataPath)
    }
  }

  private class ValueIndexerModelReader
    extends MLReader[ValueIndexerModel] {

    override def load(path: String): ValueIndexerModel = {
      val qualPath = PipelineUtilities.makeQualifiedPath(sc, path)
      // load the uid, input and output columns
      val dataPath = new Path(qualPath, dataPart).toString
      val data = sparkSession.read.format("parquet").load(dataPath)

      val Row(uid: String, dataType: DataType, inputCol: String, outputCol: String) =
        data.select("uid", "dataType", "inputCol", "outputCol").head()

      // get the levels
      val levels = ObjectUtilities.loadObject[Array[_]](qualPath, levelsPart, sc)

      new ValueIndexerModel(uid, levels, dataType, inputCol, outputCol)
    }
  }
}
