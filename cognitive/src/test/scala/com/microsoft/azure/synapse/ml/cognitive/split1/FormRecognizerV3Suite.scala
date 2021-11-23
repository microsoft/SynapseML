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
import scala.collection.mutable

object FormRecognizerV3Utils {
  def layoutTest(model: AnalyzeDocumentV3, df: DataFrame): DataFrame = {
    model.transform(df)
      .withColumn("content", col("result.analyzeResult.content"))
      .withColumn("cells", flatten(col("result.analyzeResult.tables.cells")))
      .withColumn("cells", col("cells.content"))
  }

  def modelsTest(model: AnalyzeDocumentV3, df: DataFrame, useBytes: Boolean): Array[Row] = {
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

  def documentTest(model: AnalyzeDocumentV3, df: DataFrame): DataFrame = {
    model.transform(df)
      .withColumn("content", col("result.analyzeResult.content"))
      .withColumn("entities", col("result.analyzeResult.entities.content"))
      .withColumn("keyValuePairs", col("result.analyzeResult.keyValuePairs"))
      .withColumn("keyValuePairs", map_from_arrays(col("keyValuePairs.key.content"),
        col("keyValuePairs.value.content")))
  }
}

class AnalyzeDocumentV3Suite extends TransformerFuzzing[AnalyzeDocumentV3] with FormRecognizerUtils
  with CustomModelUtils {

  import FormRecognizerV3Utils._

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "result.analyzeResult.content")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  def analyzeDocumentV3: AnalyzeDocumentV3 = new AnalyzeDocumentV3()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("result")
    .setConcurrency(5)

  lazy val analyzeLayout: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-layout")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeLayout: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-layout")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-layout Basic Usage") {
    val result1 = layoutTest(analyzeLayout, imageDf1)
      .select("source", "result", "content", "cells")
      .collect()
    assert(result1.head.getString(2).startsWith("Purchase Order\nHero Limited\nCompany Phone: 555-348-6512\n" +
      "Website: www.herolimited.com"))
    assert(result1.head.getSeq(3).mkString(";").startsWith("Details;Quantity;Unit Price;Total;Bindings;20;1.00"))

    val result2 = layoutTest(bytesAnalyzeLayout, bytesDF1)
      .select("imageBytes", "result", "content", "cells")
      .collect()
    assert(result2.head.getString(2).startsWith("Purchase Order\nHero Limited\nCompany Phone: 555-348-6512\n" +
      "Website: www.herolimited.com"))
    assert(result2.head.getSeq(3).mkString(";").startsWith("Details;Quantity;Unit Price;Total;Bindings;20;1.00"))
  }

  lazy val analyzeIDDocuments: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-idDocument")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeIDDocuments: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-idDocument")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-idDocument Basic Usage") {
    val result1 = modelsTest(analyzeIDDocuments, imageDf5, false)
    resultAssert(result1, "USA WASHINGTON\nWA\n20 1234567XX1101\nDRIVER LICENSE\nFEDERAL LIMITS APPLY",
      "Address,CountryRegion,DateOfBirth,DateOfExpiration,DocumentNumber,Endorsements,FirstName," +
        "LastName,Locale,Region,Restrictions,Sex")

    val result2 = modelsTest(bytesAnalyzeIDDocuments, bytesDF5, true)
    resultAssert(result2, "USA WASHINGTON\nWA\n20 1234567XX1101\nDRIVER LICENSE\nFEDERAL LIMITS APPLY",
      "Address,CountryRegion,DateOfBirth,DateOfExpiration,DocumentNumber,Endorsements,FirstName," +
        "LastName,Locale,Region,Restrictions,Sex")
  }

  lazy val analyzeBusinessCards: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-businessCard")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeBusinessCards: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-businessCard")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-businessCard Basic Usage") {
    val result1 = modelsTest(analyzeBusinessCards, imageDf3, false)
    resultAssert(result1, "Dr. Avery Smith\nSenior Researcher\nCloud & Al Department\navery.smith@contoso.com\n" +
      "https://www.contoso.com/\nmob:", "Addresses,CompanyNames,ContactNames,Departments,Emails,Faxes," +
      "JobTitles,Locale,MobilePhones,Websites,WorkPhones")

    val result2 = modelsTest(bytesAnalyzeBusinessCards, bytesDF3, true)
    resultAssert(result2, "Dr. Avery Smith\nSenior Researcher\nCloud & Al Department\navery.smith@contoso.com\n" +
      "https://www.contoso.com/\nmob:", "Addresses,CompanyNames,ContactNames,Departments,Emails,Faxes," +
      "JobTitles,Locale,MobilePhones,Websites,WorkPhones")
  }

  lazy val analyzeInvoices: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-invoice")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeInvoices: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-invoice")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-invoice Basic Usage") {
    val result1 = modelsTest(analyzeInvoices, imageDf4, false)
    resultAssert(result1, "Contoso\nAddress:\nInvoice For: Microsoft\n1 Redmond way Suite\n1020 Enterprise Way\n",
      "CustomerAddress,CustomerAddressRecipient,CustomerName,DueDate,InvoiceDate," +
        "InvoiceId,Items,Locale,VendorAddress,VendorName")

    val result2 = modelsTest(bytesAnalyzeInvoices, bytesDF4, true)
    resultAssert(result2, "Contoso\nAddress:\nInvoice For: Microsoft\n1 Redmond way Suite\n1020 Enterprise Way\n",
      "CustomerAddress,CustomerAddressRecipient,CustomerName,DueDate,InvoiceDate," +
        "InvoiceId,Items,Locale,VendorAddress,VendorName")
  }

  lazy val analyzeReceipts: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-receipt")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeReceipts: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-receipt")
    .setImageBytesCol("imageBytes")

  test("Prebuilt-receipt Basic Usage") {
    val result1 = modelsTest(analyzeReceipts, imageDf2, false)
    resultAssert(result1, "Contoso\nContoso\n123 Main Street\nRedmond, WA 98052\n123-456-7890\n" +
      "6/10/2019 13:59\nSales Associate", "Items,Locale,MerchantAddress,MerchantName,MerchantPhoneNumber," +
      "ReceiptType,Subtotal,Tax,Total,TransactionDate,TransactionTime")

    val result2 = modelsTest(bytesAnalyzeReceipts, bytesDF2, true)
    resultAssert(result2, "Contoso\nContoso\n123 Main Street\nRedmond, WA 98052\n123-456-7890\n" +
      "6/10/2019 13:59\nSales Associate", "Items,Locale,MerchantAddress,MerchantName,MerchantPhoneNumber," +
      "ReceiptType,Subtotal,Tax,Total,TransactionDate,TransactionTime")
  }

  lazy val analyzeDocument: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-document")
    .setImageUrlCol("source")

  lazy val bytesAnalyzeDocument: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId("prebuilt-document")
    .setImageBytesCol("imageBytes")


  test("Prebuilt-document Basic Usage") {
    val result1 = documentTest(analyzeDocument, imageDf2)
      .select("source", "result", "content", "entities", "keyValuePairs")
      .collect()
    assert(result1.head.getString(2).startsWith("Contoso\nContoso\n123 Main Street\nRedmond, WA 98052\n" +
      "123-456-7890\n6/10/2019 13:59\nSales Associate"))
    assert(result1.head.getSeq(3).mkString(",").equals("123 Main Street,123,Redmond,WA,98052,6/10/2019 " +
      "13:59,1,6 256GB,8GB,999.00,1,99.99,$ 1098.99,104.40,$ 1203.39"))
    assert(result1.head.getMap(4).toString().equals("Map(Sales Associate: -> Paul, Sub-Total -> $ 1098.99," +
      " Tax -> 104.40, Total -> $ 1203.39)"))

    val result2 = documentTest(bytesAnalyzeDocument, bytesDF2)
      .select("imageBytes", "result", "content", "entities", "keyValuePairs")
      .collect()
    assert(result2.head.getString(2).startsWith("Contoso\nContoso\n123 Main Street\nRedmond, WA 98052\n" +
      "123-456-7890\n6/10/2019 13:59\nSales Associate"))
    assert(result2.head.getSeq(3).mkString(",").equals("123 Main Street,123,Redmond,WA,98052,6/10/2019 " +
      "13:59,1,6 256GB,8GB,999.00,1,99.99,$ 1098.99,104.40,$ 1203.39"))
    assert(result2.head.getMap(4).toString().equals("Map(Sales Associate: -> Paul, Sub-Total -> $ 1098.99," +
      " Tax -> 104.40, Total -> $ 1203.39)"))
  }

  lazy val analyzeCustomModel: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId(modelId.get)
    .setImageUrlCol("source")

  lazy val bytesAnalyzeCustomModel: AnalyzeDocumentV3 = analyzeDocumentV3
    .setPrebuiltModelId(modelId.get)
    .setImageBytesCol("imageBytes")

  override def testObjects(): Seq[TestObject[AnalyzeDocumentV3]] =
    Seq(new TestObject(analyzeLayout, imageDf1))

  override def reader: MLReadable[_] = AnalyzeDocumentV3
}
