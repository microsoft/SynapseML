package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import org.apache.spark.ml.param.{BooleanParam, Params}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

trait HasReturnUsage extends Params {
  import HasReturnUsage._

  final val returnUsage: BooleanParam = new BooleanParam(
    this,
    "returnUsage",
    "Whether to include usage statistics alongside the response output."
  )

  setDefault(returnUsage -> false)

  final def getReturnUsage: Boolean = $(returnUsage)

  final def setReturnUsage(value: Boolean): this.type = set(returnUsage, value)

  protected def usageStructType: StructType = NormalizedUsage.schema

  protected def normalizeUsageColumn(column: org.apache.spark.sql.Column,
                                     mapping: UsageFieldMapping): org.apache.spark.sql.Column = {
    HasReturnUsage.normalizationUdf(mapping)(column)
  }
}

object HasReturnUsage {

  case class UsageFieldMapping(inputTokens: Option[String],
                               outputTokens: Option[String],
                               totalTokens: Option[String] = Some("total_tokens"),
                               inputDetails: Option[String] = None,
                               outputDetails: Option[String] = None) extends Serializable

  case class NormalizedUsage(input_tokens: java.lang.Long,
                             output_tokens: java.lang.Long,
                             total_tokens: java.lang.Long,
                             input_token_details: Map[String, Long],
                             output_token_details: Map[String, Long]) extends Serializable

  object NormalizedUsage extends SparkBindings[NormalizedUsage]

  private def toLong(value: Any): Option[Long] = value match {
    case n: java.lang.Number => Some(n.longValue())
    case n: Number => Some(n.longValue())
    case _ => None
  }

  private def rowToLongMap(row: Row): Map[String, Long] = {
    row.schema.fields.zipWithIndex.flatMap { case (field, idx) =>
      toLong(row.get(idx)).map(field.name -> _)
    }.toMap
  }

  private def hasField(row: Row, name: String): Boolean =
    row.schema.fieldNames.contains(name)

  private def normalize(usage: Row, mapping: UsageFieldMapping): NormalizedUsage = {
    if (usage == null) {
      NormalizedUsage(null, null, null, Map.empty, Map.empty)
    } else {
      def fieldValue(fieldOpt: Option[String]): java.lang.Long =
        fieldOpt.filter(name => hasField(usage, name))
          .flatMap(name => toLong(usage.getAs[Any](name)))
          .map(java.lang.Long.valueOf)
          .orNull

      def detailMap(fieldOpt: Option[String]): Map[String, Long] =
        fieldOpt.filter(name => hasField(usage, name))
          .flatMap(name => Option(usage.getAs[Row](name)))
          .map(rowToLongMap)
          .getOrElse(Map.empty[String, Long])

      NormalizedUsage(
        fieldValue(mapping.inputTokens),
        fieldValue(mapping.outputTokens),
        fieldValue(mapping.totalTokens),
        detailMap(mapping.inputDetails),
        detailMap(mapping.outputDetails)
      )
    }
  }

  private val udfCache =
    new java.util.concurrent.ConcurrentHashMap[UsageFieldMapping, UserDefinedFunction]()

  private[openai] def normalizationUdf(mapping: UsageFieldMapping): UserDefinedFunction = {
    udfCache.computeIfAbsent(
      mapping,
      _ => udf((usage: Row) => normalize(usage, mapping))
    )
  }
}
