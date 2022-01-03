// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructType}
import org.scalactic.Equality

class FormOntologyLearnerSuite extends EstimatorFuzzing[FormOntologyLearner] with FormRecognizerUtils {

  import spark.implicits._

  lazy val analyzeInvoices: AnalyzeInvoices = new AnalyzeInvoices()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("invoices")
    .setConcurrency(5)

  lazy val ontologyLearner: FormOntologyLearner = new FormOntologyLearner()
    .setInputCol("invoices")
    .setOutputCol("unified_ontology")

  lazy val urlDF: DataFrame = Seq(
    "https://mmlsparkdemo.blob.core.windows.net/ignite2021/forms/2017/Invoice115991.pdf",
    "https://mmlsparkdemo.blob.core.windows.net/ignite2021/forms/2018/Invoice119554.pdf",
    "https://mmlsparkdemo.blob.core.windows.net/ignite2021/forms/2009/Invoice12241.pdf"
  ).toDF("url")

  lazy val df: DataFrame = analyzeInvoices.transform(urlDF).cache()

  test("Basic Usage") {

    val targetSchema = new StructType()
      .add("CustomerAddress", StringType)
      .add("InvoiceTotal", DoubleType)
      .add("CustomerName", StringType)
      .add("VendorName", StringType)
      .add("VendorAddressRecipient", StringType)
      .add("InvoiceId", StringType)
      .add("InvoiceDate", StringType)
      .add("SubTotal", DoubleType)
      .add("TotalTax", DoubleType)
      .add("CustomerAddress", StringType)
      .add("VendorAddress", StringType)
      .add("ShippingAddress", StringType)
      .add("CustomerAddressRecipient", StringType)
      .add("Items", ArrayType(new StructType()
        .add("ProductCode", StringType)
        .add("Tax", DoubleType)
        .add("Quantity", DoubleType)
        .add("UnitPrice", DoubleType)
        .add("Description", StringType)
        .add("Amount", DoubleType)
      ))

    val newDF = ontologyLearner.fit(df).transform(df)
    assert(newDF.select("unified_ontology.*").schema.fieldNames.toSet ===
      targetSchema.fieldNames.toSet)
    assert(
      newDF.select("unified_ontology.Items").schema("Items")
        .dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames.toSet ===
        targetSchema("Items").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames.toSet)
    assert(newDF.select("unified_ontology.*").collect().head.getAs[Double]("TotalTax") === 67.13)
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("url", "unified_ontology.SubTotal")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  override def testObjects(): Seq[TestObject[FormOntologyLearner]] =
    Seq(new TestObject(ontologyLearner, df))

  override def reader: MLReadable[_] = FormOntologyLearner

  override def modelReader: MLReadable[_] = FormOntologyTransformer

}
