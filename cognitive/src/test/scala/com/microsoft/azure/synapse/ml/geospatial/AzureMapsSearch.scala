// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait AzureMapsKey {
  lazy val azureMapsKey: String = sys.env.getOrElse("AZURE_MAPS_KEY", Secrets.AzureMapsKey)
}

class AzureMapSearchSuite extends TransformerFuzzing[AddressGeocoder] with AzureMapsKey {

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
    "Chile{Santiago,Santiago Metropolitan}-Schelmenwasenstraße Stuttgart 70567 Baden-Württemberg DE",
    "United States{Ozark,Alabama}-1014 Indian Pass Rd, Port St Joe, FL 32456"
  ).toDF("address")

  lazy val batchGeocodeAddresses: AddressGeocoder = new AddressGeocoder()
    .setSubscriptionKey(azureMapsKey)
    .setAddressCol("address")
    .setOutputCol("output")

  test("Basic Batch Geocode Usage") {
    val batchedDF = batchGeocodeAddresses.transform(new FixedMiniBatchTransformer().setBatchSize(5).transform(df))
    val flattenedResults = new FlattenBatch().transform(batchedDF)
      .select(
        col("address"),
        col("output.response.results").getItem(0).getField("position")
          .getField("lat").as("latitude"),
        col("output.response.results").getItem(0).getField("position")
          .getField("lon").as("longitude"))
      .collect()

    assert(flattenedResults != null)
    assert(flattenedResults.length == 10)
  }

  override def testObjects(): Seq[TestObject[AddressGeocoder]] =
    Seq(new TestObject[AddressGeocoder](batchGeocodeAddresses, df))

  override def reader: MLReadable[_] = AddressGeocoder
}
