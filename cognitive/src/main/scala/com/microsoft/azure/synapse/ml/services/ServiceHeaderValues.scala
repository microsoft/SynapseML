// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

private[ml] object ServiceHeaderValues {

  private def values(value: Option[Any]): Iterator[Any] = value.iterator.flatMap {
    case batch: Seq[_] => batch.iterator
    case scalar => Iterator.single(scalar)
  }.flatMap(value => Option(value))

  def stringValue(value: Option[Any], paramName: String): Option[String] = {
    values(value).map {
      case stringValue: String => stringValue
      case _ => throw new IllegalArgumentException(
        s"Header service parameter '$paramName' must reference a String or array<string> column")
    }.find(ServiceAuthHeaders.nonBlank)
  }

  def mapValue(value: Option[Any], paramName: String): Option[Map[String, String]] = {
    values(value).map {
      case mapValue: scala.collection.Map[_, _] =>
        if (mapValue.exists { case (name, value) =>
          Option(name).exists(headerName => !headerName.isInstanceOf[String]) ||
            Option(value).exists(headerValue => !headerValue.isInstanceOf[String])
        }) {
          throw invalidMapType(paramName)
        }
        ServiceAuthHeaders.sanitizeHeaderMap(mapValue.iterator.collect {
          case (name: String, headerValue: String) => name -> headerValue
        }.toMap)
      case _ => throw invalidMapType(paramName)
    }.find(_.nonEmpty)
  }

  private def invalidMapType(paramName: String): IllegalArgumentException = {
    new IllegalArgumentException(
      s"Header service parameter '$paramName' must reference a map<string,string> " +
        "or array<map<string,string>> column")
  }
}
