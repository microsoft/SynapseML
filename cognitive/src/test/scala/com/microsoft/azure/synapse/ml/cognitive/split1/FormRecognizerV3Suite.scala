// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.scalactic.Equality

import scala.collection.immutable.HashMap

object FormRecognizerV3Utils {
  def layoutTest(model: AnalyzeDocument, df: DataFrame): DataFrame = {
    model.transform(df)
      .withColumn("content", col("result.analyzeResult.content"))
      .withColumn("cells", flatten(col("result.analyzeResult.tables.cells")))
      .withColumn("cells", col("cells.content"))
  }

  def modelsTest(model: AnalyzeDocument, df: DataFrame, useBytes: Boolean): Array[Row] = {
    val transDF = model.transform(df)
      .withColumn("content", col("result.analyzeResult.content"))
      .withColumn("fields", col("result.analyzeResult.documents.fields"))
    if (!useBytes) {
      transDF.select("source", "result", "content", "fields").collect()
    } else {
      transDF.select("imageBytes", "result", "content", "fields").collect()
    }
  }

  def resultAssert(result: Array[Row], str1: String, str2: String): Unit = {
    assert(result.head.getString(2).startsWith(str1))
    assert(result.head.getSeq(3).head.asInstanceOf[HashMap.HashTrieMap[String, _]]
      .keys.toSeq.sortWith(_ < _).mkString(",").equals(str2))
  }

  def documentTest(model: AnalyzeDocument, df: DataFrame): DataFrame = {
    model.transform(df)
      .withColumn("content", col("result.analyzeResult.content"))
      .withColumn("entities", col("result.analyzeResult.entities.content"))
      .withColumn("keyValuePairs", col("result.analyzeResult.keyValuePairs"))
      .withColumn("keyValuePairs", map_from_arrays(col("keyValuePairs.key.content"),
        col("keyValuePairs.value.content")))
  }
}

class AnalyzeDocumentSuite extends TransformerFuzzing[AnalyzeDocument] with FormRecognizerUtils
  with CustomModelUtils {

  import FormRecognizerV3Utils._

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "result.analyzeResult.content")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("basic usage with tables") {
    val fromRow = AnalyzeDocumentResponse.makeFromRowConverter
    analyzeDocument
      .setPrebuiltModelId("prebuilt-layout")
      .setImageUrlCol("source")
      .transform(imageDf6)
      .collect()
      .map(r => fromRow(r.getAs[Row]("result")))
      .foreach(r => assert(r.analyzeResult.pages.get.head.pageNumber >= 0))
  }

  def analyzeDocument: AnalyzeDocument = new AnalyzeDocument()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("result")
    .setConcurrency(5)

  lazy val analyzeRead: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-read")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeRead: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-read")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-read Basic Usage") {
    val result1 = analyzeRead.transform(imageDf1)
      .withColumn("content", col("result.analyzeResult.content"))
      .select("source", "content")
      .collect()

    val result2 = bytesAnalyzeRead.transform(bytesDF1)
      .withColumn("content", col("result.analyzeResult.content"))
      .select("imageBytes", "content")
      .collect()

    for (result <- Seq(result1, result2)) {
      assert(result.head.getString(1).startsWith("Purchase Order\nHero Limited\nCompany Phone: 555-348-6512\n"))
    }
  }

  lazy val analyzeLayout: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-layout")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeLayout: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-layout")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-layout Basic Usage") {
    val result1 = layoutTest(analyzeLayout, imageDf1)
      .select("source", "result", "content", "cells")
      .collect()
    val result2 = layoutTest(bytesAnalyzeLayout, bytesDF1)
      .select("imageBytes", "result", "content", "cells")
      .collect()

    for (result <- Seq(result1, result2)) {
      assert(result.head.getString(2).startsWith("Purchase Order\nHero Limited\nCompany Phone: 555-348-6512\n" +
        "Website: www.herolimited.com"))
      assert(result.head.getSeq(3).mkString(";").startsWith("Details;Quantity;Unit Price;Total;Bindings;20;1.00"))
    }
  }

  lazy val analyzeIDDocuments: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-idDocument")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeIDDocuments: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-idDocument")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-idDocument Basic Usage") {
    val result1 = modelsTest(analyzeIDDocuments, imageDf5, false)
    val result2 = modelsTest(bytesAnalyzeIDDocuments, bytesDF5, true)
    for (result <- Seq(result1, result2)) {
      resultAssert(result, "WA WASHINGTON\n20 1234567XX1101\nDRIVER LICENSE\nFEDERAL LIMITS APPLY\n" +
        "4d LIC#WDLABCD456DG 9CLASS\nDONORS\n1 TALBOT\n2 LIAM R.\n3 DOB 01/06/1958\n",
        "Address,CountryRegion,DateOfBirth,DateOfExpiration,DocumentNumber," +
          "Endorsements,FirstName,LastName,Region,Restrictions,Sex")
    }
  }

  lazy val analyzeBusinessCards: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-businessCard")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeBusinessCards: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-businessCard")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-businessCard Basic Usage") {
    val result1 = modelsTest(analyzeBusinessCards, imageDf3, false)
    val result2 = modelsTest(bytesAnalyzeBusinessCards, bytesDF3, true)
    for (result <- Seq(result1, result2)) {
      resultAssert(result, "Dr. Avery Smith\nSenior Researcher\nCloud & Al Department\n" +
        "avery.smith@contoso.com\nhttps://www.contoso.com/\nmob:", "Addresses,CompanyNames,ContactNames," +
        "Departments,Emails,Faxes,JobTitles,Locale,MobilePhones,Websites,WorkPhones")
    }
  }

  lazy val analyzeInvoices: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-invoice")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeInvoices: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-invoice")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-invoice Basic Usage") {
    val result1 = modelsTest(analyzeInvoices, imageDf4, false)
    val result2 = modelsTest(bytesAnalyzeInvoices, bytesDF4, true)
    for (result <- Seq(result1, result2)) {
      resultAssert(result, "Contoso\nAddress:\n1 Redmond way Suite\n6000 Redmond, WA\n99243\n" +
        "Invoice For: Microsoft\n1020 Enterprise Way", "CustomerAddress,CustomerAddressRecipient," +
        "CustomerName,DueDate,InvoiceDate,InvoiceId,Items,VendorAddress,VendorName")
    }
  }

  lazy val analyzeReceipts: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-receipt")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeReceipts: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-receipt")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-receipt Basic Usage") {
    val result1 = modelsTest(analyzeReceipts, imageDf2, false)
    val result2 = modelsTest(bytesAnalyzeReceipts, bytesDF2, true)
    for (result <- Seq(result1, result2)) {
      resultAssert(result, "O\nContoso\nContoso\n123 Main Street\nRedmond, WA 98052\n123-456-7890\n" +
        "6/10/2019 13:59\nSales Associate: Paul\n", "Items,Locale,MerchantAddress,MerchantName," +
        "MerchantPhoneNumber,Subtotal,Total,TotalTax,TransactionDate,TransactionTime")
    }
  }

  lazy val analyzePrebuiltDocument: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-document")
    .setImageUrlCol("source")

  lazy val bytesAnalyzePrebuiltDocument: AnalyzeDocument = analyzeDocument
    .setPrebuiltModelId("prebuilt-document")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-document Basic Usage") {
    val result1 = documentTest(analyzePrebuiltDocument, imageDf2)
      .select("source", "result", "content", "entities", "keyValuePairs")
      .collect()
    val result2 = documentTest(bytesAnalyzePrebuiltDocument, bytesDF2)
      .select("imageBytes", "result", "content", "entities", "keyValuePairs")
      .collect()

    for (result <- Seq(result1, result2)) {
      assert(result.head.getString(2).startsWith("O\nContoso\nContoso\n123 Main Street\nRedmond, WA 98052\n" +
        "123-456-7890\n6/10/2019 13:59\nSales Associate: Paul\n"))
      assert(result.head.getSeq(3).mkString(",").equals("8GB,999.00,1,99.99,- 1098.99,$ 104.40,1203.39,Contoso," +
        "Contoso,123 Main Street,123,Redmond,WA,98052,6/10/2019 13:59,Paul,-\n 1,6,256GB,Intel"))
      assert(result.head.getMap(4).toString().equals("Map(Tax -> $ 104.40, 1 Surface Pro 6 256GB /" +
        " Intel Core i5 / 8GB RAM (Black) -> 999.00, 1 SurfacePen -> 99.99, Total -> 1203.39, " +
        "Sub-Total -> 1098.99, Sales Associate: -> Paul)"))
    }
  }

  override def testObjects(): Seq[TestObject[AnalyzeDocument]] =
    Seq(new TestObject(analyzeLayout, imageDf1))

  override def reader: MLReadable[_] = AnalyzeDocument
}
