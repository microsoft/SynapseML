// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.{DenseVector, SQLDataTypes, SparseVector, Vector, Vectors}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{GetStructField, KnownNotNull}
import org.apache.spark.sql.functions.{lit, struct, transform, transform_keys, transform_values, when}
import org.apache.spark.sql.types._

/** Preserve Spark values and schema while input rows traverse inference encoders. */
private[ai] object AIValueCodec {
  private val VectorStorage = StructType(Seq(
    StructField("sparse", BooleanType),
    StructField("size", IntegerType),
    StructField("indices", ArrayType(IntegerType, containsNull = true)),
    StructField("values", ArrayType(DoubleType, containsNull = true))))
  def nullable(dataType: DataType): DataType = dataType match {
    case s: StructType => StructType(s.fields.map(f => f.copy(dataType = nullable(f.dataType), nullable = true)))
    case ArrayType(t, _) => ArrayType(nullable(t), containsNull = true)
    case MapType(k, v, _) => MapType(nullable(k), nullable(v), valueContainsNull = true)
    case other => other
  }

  def validateMapKeys(dataType: DataType, inKey: Boolean = false): Unit = dataType match {
    case s: StructType => s.fields.foreach(field => validateMapKeys(field.dataType, inKey))
    case ArrayType(t, _) => validateMapKeys(t, inKey)
    case MapType(k, v, _) => validateMapKeys(k, inKey = true); validateMapKeys(v, inKey)
    case t: UserDefinedType[_] =>
      require(!inKey || t == SQLDataTypes.VectorType,
        "AIPrompt supports only Spark ML vectors as UDT map keys; other UDT keys can lose SQL identity.")
    case _ =>
  }

  def encodeMapKeys(frame: DataFrame): DataFrame = convertMapKeyColumns(frame, frame.schema, encode = true)

  def decodeMapKeys(frame: DataFrame, schema: StructType): DataFrame =
    convertMapKeyColumns(frame, schema, encode = false)

  def reconcileSchema(frame: DataFrame, schema: StructType): DataFrame = {
    val columns = schema.fields.map { field =>
      val source = frame.schema(field.name)
      require(DataType.equalsIgnoreNullability(source.dataType, field.dataType),
        "AIPrompt output types do not match the planned schema")
      val column = AIColumnPlan.column(field.name)
      // Struct casts preserve ordinal fields; Dataset.to resolves nested names and can introduce ambiguity.
      val converted = if (source.dataType == field.dataType) column else column.cast(field.dataType)
      val widened = if (field.nullable && !source.nullable) when(lit(true), converted) else converted
      widened.as(field.name, field.metadata)
    }
    val result = frame.select(columns: _*)
    require(result.schema == schema, "AIPrompt output schema does not match the planned schema")
    result
  }

  private def containsVectorMapKey(dataType: DataType): Boolean = dataType match {
    case s: StructType => s.fields.exists(field => containsVectorMapKey(field.dataType))
    case ArrayType(t, _) => containsVectorMapKey(t)
    case MapType(k, v, _) => containsVector(k) || containsVectorMapKey(v)
    case _ => false
  }

  private def convertMapKeyColumns(frame: DataFrame, schema: StructType, encode: Boolean): DataFrame = {
    val converted = schema.fields.filter(field => containsVectorMapKey(field.dataType)).map(_.name).toSet
    if (converted.isEmpty) frame else frame.select(schema.fields.map { field =>
      val column = AIColumnPlan.column(field.name)
      if (converted(field.name)) {
        convertColumn(column, field.dataType, field.nullable, encode).as(field.name, field.metadata)
      } else column
    }: _*)
  }

  private def containsVector(dataType: DataType): Boolean = dataType match {
    case t if t == SQLDataTypes.VectorType => true
    case s: StructType => s.fields.exists(field => containsVector(field.dataType))
    case ArrayType(t, _) => containsVector(t)
    case MapType(k, v, _) => containsVector(k) || containsVector(v)
    case _ => false
  }

  private def convertVectorColumn(column: Column, nullable: Boolean, encode: Boolean): Column = {
    val conversion = if (encode) {
      UDFUtils.oldUdf((value: Vector) => Option(value).map(encodeVector).orNull, VectorStorage)
    } else {
      UDFUtils.oldUdf((value: Row) => Option(value).map(decodeVector).orNull, SQLDataTypes.VectorType)
    }
    (if (nullable) conversion else conversion.asNonNullable())(column)
  }

  private def convertMapColumn(column: Column, dataType: MapType, encode: Boolean): Column = {
    // Catalyst preserves SQL key identity; JVM vector equality can collapse dense and sparse keys.
    val values = if (containsVector(dataType.valueType)) {
      transform_values(column, (_, value) =>
        convertColumn(value, dataType.valueType, dataType.valueContainsNull, encode))
    } else column
    if (containsVector(dataType.keyType)) {
      transform_keys(values, (key, _) => convertColumn(key, dataType.keyType, nullable = false, encode = encode))
    } else values
  }

  private def convertStructColumn(column: Column, schema: StructType, nullable: Boolean, encode: Boolean): Column = {
    val fields = schema.fields.zipWithIndex.map { case (field, index) =>
      val extracted = GetStructField(column.expr, index, Some(field.name))
      // The parent-null guard makes declared non-null children safe to retain as non-null.
      val child = new Column(if (field.nullable) extracted else KnownNotNull(extracted))
      convertColumn(child, field.dataType, field.nullable, encode).as(field.name, field.metadata)
    }
    val converted = struct(fields: _*)
    if (nullable) when(column.isNotNull, converted) else converted
  }

  private def convertColumn(column: Column, dataType: DataType, nullable: Boolean, encode: Boolean): Column = {
    if (!containsVector(dataType)) column else dataType match {
      case t if t == SQLDataTypes.VectorType => convertVectorColumn(column, nullable, encode)
      case s: StructType => convertStructColumn(column, s, nullable, encode)
      case ArrayType(t, containsNull) =>
        transform(column, value => convertColumn(value, t, containsNull, encode))
      case map: MapType => convertMapColumn(column, map, encode)
      case _ => column
    }
  }

  private def encodeVector(vector: Vector): Row = vector match {
    case sparse: SparseVector => Row(true, sparse.size, sparse.indices.toSeq, sparse.values.toSeq)
    case dense: DenseVector => Row(false, dense.size, Seq.empty[Int], dense.values.toSeq)
  }

  private def decodeVector(row: Row): Vector = {
    require(row.toSeq.forall(value => Option(value).isDefined), "Invalid encoded AIPrompt vector fields.")
    val size = row.getInt(1)
    val rawIndices = row.getSeq[Any](2)
    val rawValues = row.getSeq[Any](3)
    require((rawIndices ++ rawValues).forall(value => Option(value).isDefined),
      "Invalid encoded AIPrompt vector elements.")
    val indices = rawIndices.map(_.asInstanceOf[Int]).toArray
    val values = rawValues.map(_.asInstanceOf[Double]).toArray
    require(size >= 0, "Invalid encoded AIPrompt vector size.")
    if (row.getBoolean(0)) {
      require(indices.length == values.length && indices.forall(i => i >= 0 && i < size) &&
        indices.sliding(2).forall(pair => pair.length < 2 || pair.head < pair.last),
        "Invalid encoded AIPrompt sparse vector.")
      Vectors.sparse(size, indices, values)
    } else {
      require(indices.isEmpty && values.length == size, "Invalid encoded AIPrompt dense vector.")
      Vectors.dense(values)
    }
  }
}
