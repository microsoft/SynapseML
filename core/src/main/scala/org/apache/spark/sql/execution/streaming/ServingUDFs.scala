// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.execution.streaming

import com.microsoft.azure.synapse.ml.io.http.HTTPResponseData
import com.microsoft.azure.synapse.ml.io.http.HTTPSchema.{binary_to_response, empty_response, string_to_response}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.sql.execution.streaming.continuous.HTTPSourceStateHolder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, struct, to_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

object ServingUDFs {

  private def jsonReply(c: Column) = string_to_response(to_json(c))

  def makeReplyUDF(data: Column,  //scalastyle:off cyclomatic.complexity
                   dt: DataType,
                   code: Column = lit(200), //scalastyle:ignore magic.number
                   reason: Column = lit("Success")): Column = {
    dt match {
      case NullType => empty_response(code, reason)
      case StringType => string_to_response(data, code, reason)
      case BinaryType => binary_to_response(data)
      case _: StructType => jsonReply(data)
      case _: MapType => jsonReply(data)
      case at: ArrayType => at.elementType match {
        case _: StructType => jsonReply(data)
        case _: MapType => jsonReply(data)
        case _ => jsonReply(struct(data))
      }
      case _ => jsonReply(struct(data))
    }
  }

  private def sendReplyHelper(mapper: Row => HTTPResponseData)(serviceName: String, reply: Row, id: Row): Boolean = {
    if (Option(reply).isEmpty || Option(id).isEmpty) {
      null.asInstanceOf[Boolean] //scalastyle:ignore null
    } else {
      Try(HTTPSourceStateHolder.getServer(serviceName).replyTo(id.getString(0), id.getString(1), mapper(reply)))
        .toOption.isDefined
    }
  }

  def sendReplyUDF: UserDefinedFunction = {
    val toData = HTTPResponseData.makeFromRowConverter
    UDFUtils.oldUdf(sendReplyHelper(toData) _, BooleanType)
  }

}
