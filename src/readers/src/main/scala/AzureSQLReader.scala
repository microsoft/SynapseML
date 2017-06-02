// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.NoSuchElementException
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import scala.util.parsing.json.JSON

object AzureSQLReader {
  def read(serverName: String, databaseName: String, query: String, userName: String, password: String): DataFrame = {
    // val spark = SQLContext.getOrCreate(null)
    val spark = SparkSession.builder.getOrCreate()

    // Convert query to subquery
    val subQueryPrefix = "("
    val subQueryPostfix = ") AS mmlTempTable123"
    val subQuery = subQueryPrefix.concat(query).concat(subQueryPostfix)
    println(subQuery)
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val urlPrefix = "jdbc:sqlserver://"
    val urlPostfix = ".database.windows.net"
    val url = urlPrefix.concat(serverName).concat(urlPostfix)
    val options = Map("url" -> url, "databaseName" -> databaseName, "driver" -> driver, "dbtable" -> subQuery,
                      "user" -> userName, "password" -> password)

    spark.read.format("jdbc").options(options).load()
  }

  def read2 (jsonStr: String): DataFrame = {
    val parsedJsonStr = JSON.parseFull(jsonStr)
    var serverName= ""
    var databaseName = ""
    var query = ""
    var userName = ""
    var password = ""

    try {
      password = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("password").asInstanceOf[String]
      userName = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("userName").asInstanceOf[String]
      query = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("query").asInstanceOf[String]
      databaseName = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("databaseName").asInstanceOf[String]
      serverName = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("serverName").asInstanceOf[String]
    } catch {
      case ex: NoSuchElementException => {
        throw new IllegalArgumentException("parameter not found or invalid Json format detected in the input.")
      }
    }

    read(serverName, databaseName, query, userName, password)
  }
}
