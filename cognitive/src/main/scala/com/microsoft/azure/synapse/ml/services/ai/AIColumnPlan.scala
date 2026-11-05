// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.sql.{Column, Row, functions => F}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

import java.util.{HashSet, Set => JavaSet, TreeSet}

private[ai] final case class AIColumnPlan(input: StructType, outputs: Seq[StructField]) {
  AIColumnPlan.distinct(input.fieldNames.toSeq, "input columns")
  AIColumnPlan.distinct(outputs.map(_.name), "output roles")
  require(outputs.forall(_.name.nonEmpty), "output roles must have non-empty names")
  private val outputNames = outputs.map(_.name).toSet
  private val replacements = input.fields.map(f =>
    outputs.find(o => AIColumnPlan.matches(f.name, o.name)))
  val schema: StructType = StructType(input.fields.zip(replacements).map {
    case (original, replacement) => replacement.getOrElse(original)
  } ++ outputs.filterNot(o => input.fields.exists(f => AIColumnPlan.matches(f.name, o.name))))

  private val indices = schema.fields.map(f =>
    if (outputNames(f.name)) -1 else input.fieldIndex(f.name))

  def assemble(row: Row, values: Map[String, Any]): Row =
    Row.fromSeq(schema.fields.zip(indices).map { case (field, index) =>
      if (index < 0) values.getOrElse(field.name, None.orNull) else row.get(index)
    })

  def scratch(prefix: String, additional: String*): String =
    AIColumnPlan.unused(prefix, schema.fieldNames.toSeq ++ input.fieldNames ++ additional)
}

private[ai] object AIColumnPlan {
  def matches(left: String, right: String): Boolean = SQLConf.get.resolver(left, right)

  def distinct(names: Seq[String], description: String): Unit = {
    require(names.forall(_ != null), s"$description must not have null names")
    // This comparator matches Spark's equalsIgnoreCase, including Unicode variants lost by lowercasing.
    val seen: JavaSet[String] = if (SQLConf.get.caseSensitiveAnalysis) new HashSet[String]()
    else new TreeSet[String](String.CASE_INSENSITIVE_ORDER)
    require(!names.exists(name => !seen.add(name)),
      s"Ambiguous $description under Spark's column resolver")
  }

  def resolve(schema: StructType, name: String): String =
    schema.fieldNames.find(matches(_, name)).getOrElse(
      throw new IllegalArgumentException(s"Input column '$name' does not exist"))

  def unused(prefix: String, names: Seq[String]): String =
    (Iterator(prefix) ++ Iterator.from(1).map(i => s"${prefix}_$i"))
      .find(candidate => !names.exists(matches(_, candidate))).get

  def column(name: String): Column = F.col("`" + name.replace("`", "``") + "`")

  // Spark can eliminate serialization between Dataset maps and retain external JVM arrays.
  def arrayValues(value: Any): scala.collection.Seq[Any] = value match {
    case values: scala.collection.Seq[_] => values
    case values: Array[_] => values.toSeq
    case _ => throw new IllegalArgumentException("Expected a Spark array value")
  }

  // Fused Dataset maps can pass schema-less Rows directly to the protocol serializers.
  def withSchema(row: Row, schema: StructType): Row =
    new GenericRowWithSchema(schema.fields.zipWithIndex.map { case (field, index) =>
      nestedSchema(row.get(index), field.dataType)
    }, schema)

  private def nestedSchema(value: Any, dataType: DataType): Any = Option(value).map { present =>
    dataType match {
      case struct: StructType => withSchema(present.asInstanceOf[Row], struct)
      case ArrayType(element, _) =>
        arrayValues(present).iterator.map(nestedSchema(_, element)).toVector
      case MapType(key, element, _) =>
        present.asInstanceOf[scala.collection.Map[Any, Any]].iterator.map { case (k, v) =>
          nestedSchema(k, key) -> nestedSchema(v, element)
        }.toMap
      case _ => present
    }
  }.orNull
}
