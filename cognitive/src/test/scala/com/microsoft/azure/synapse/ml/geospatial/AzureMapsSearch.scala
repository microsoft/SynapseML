// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

trait AzureMapsKey {
  lazy val azureMapsKey = sys.env.getOrElse("AZURE_MAPS_KEY", Secrets.AzureMapsKey)
}

class AzureMapBatchSearchSuite extends TransformerFuzzing[BatchSearchAddress] with AzureMapsKey{
  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "Finland{Helsinki,Uusimaa}-Katajatie 7, 04260 Kerava, Suomi",
    "57{Polverigi,Marches}-Via Roma 261, Via Roma, Palermo, PA",
    "SAM{Samara,Samara}-Артемовская улица, 17, Самара",
    "Yorkshire And The H{Sheffield,Sheffield}-Alconbury Hill, Huntingdon PE28 4HY",
    "Taiwan{Hualien,Taiwan}-玉里鎮城東六街26號",
    "Indonesia{Jakarta,Jakarta}-jalan haji abah no 1 kelurahan pinang kecamatan pinang tangerang",
    "Serbia{Belgrade,Belgrade}-Ulica Aleksinackih rudara 10a, 11070 Beograd, Srbija",
    "Chile{Santiago,Region Metropolitana}-Domingo Campos Lagos 1887",
    "Finland{Helsinki,Uusimaa}-Riistakatu, Salo",
    "United States{Ozark,Alabama}-1014 Indian Pass Rd, Port St Joe, FL 32456",
  ).toDF("addresses")

  lazy val batchSearchMaps: BatchSearchAddress = new BatchSearchAddress()
    .setSubscriptionKey(azureMapsKey)
    .setAddressesCol("addresses")
    .setOutputCol("output")


  test("Basic Batch Geocode Usage") {
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(5).transform(df.coalesce(2))
    val flattened = new FlattenBatch().transform(batchSearchMaps.transform(batchedDF)).collect()
    print("flattened")
  }

  override def testObjects(): Seq[TestObject[BatchSearchAddress]] =
    Seq(new TestObject[BatchSearchAddress](batchSearchMaps, df))

  override def reader: MLReadable[_] = BatchSearchAddress
}

class AzureMapsGetSearchSuite extends TransformerFuzzing[SearchAddress] with AzureMapsKey{
  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "United Kingdom{Wolverhampton,West Midlands}-Walsall, WV14 8ET, United Kingdom",
    "United Kingdom{Sheffield,South Yorkshire}-148 burngreave road s3",
    "LA{Natchitoches,Louisiana}-97 American Way, Natchitoches, LA 71457",
    "JAL{Tonala,Jalisco}-403 Rosedell St, Amity, OR 97101, EE. UU",
    "France{Paris,Ile-De-France}-AVENUE FRIEDLAND PARIS"
  ).toDF("address")

  lazy val searchMaps: SearchAddress = new SearchAddress()
    .setSubscriptionKey(azureMapsKey)
    .setAddressCol("address")
    .setOutputCol("output")


  test("Basic Get Search Address Usage") {
    val results = searchMaps.transform(df).select(col("address"),
        col("output.results").getItem(0).getField("position").getField("lat"),
        col("output.results").getItem(0).getField("position").getField("lon"),
      ).collect()
    assert(results != null)
  }

  override def testObjects(): Seq[TestObject[SearchAddress]] =
    Seq(new TestObject[SearchAddress](searchMaps, df))

  override def reader: MLReadable[_] = SearchAddress
}