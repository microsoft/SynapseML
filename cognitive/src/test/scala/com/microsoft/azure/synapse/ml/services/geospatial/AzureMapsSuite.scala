// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.geospatial

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.services.URLEncodingUtils
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.services.geospatial.AzureMapsJsonProtocol._
import com.microsoft.azure.synapse.ml.io.http.{HeaderValues, RESTHelpers}
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch}
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalactic.Equality

import java.net.URI

trait AzureMapsKey {
  lazy val azureMapsKey: String = sys.env.getOrElse("AZURE_MAPS_KEY", Secrets.AzureMapsKey)
}

class AzMapsSearchAddressSuite extends TransformerFuzzing[AddressGeocoder] with AzureMapsKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "One, Microsoft Way, Redmond",
    "Katajatie 7, 04260 Kerava, Suomi",
    "Via Roma 261, Via Roma, Palermo, PA",
    "Артемовская улица, 17, Самара",
    "Alconbury Hill, Huntingdon PE28 4HY",
    "玉里鎮城東六街26號",
    "jalan haji abah no 1 kelurahan pinang kecamatan pinang tangerang",
    "Ulica Aleksinackih rudara 10a, 11070 Beograd, Srbija",
    "Domingo Campos Lagos 1887",
    "Schelmenwasenstraße Stuttgart 70567 Baden-Württemberg DE",
    "1014 Indian Pass Rd, Port St Joe, FL 32456",
    "400 Broad St, Seattle",
    "350 5th Ave, New York",
    "Pike Pl, Seattle",
    "Champ de Mars, 5 Avenue Anatole France, 75007 Paris"
  ).toDF("address")

  lazy val batchGeocodeAddresses: AddressGeocoder = new AddressGeocoder()
    .setSubscriptionKey(azureMapsKey)
    .setAddressCol("address")
    .setOutputCol("output")
    .setErrorCol("errors")

  def extractFields(df: DataFrame): DataFrame = {
    df.select(
      col("address"),
      col("output.response.results").getItem(0).getField("position")
        .getField("lat").as("latitude"),
      col("output.response.results").getItem(0).getField("position")
        .getField("lon").as("longitude"))
  }

  test("Basic Batch Geocode Usage") {
    val batchedDF = batchGeocodeAddresses.transform(new FixedMiniBatchTransformer().setBatchSize(5)
      .transform(df.coalesce(1)))
    val flattenedResults = extractFields(new FlattenBatch().transform(batchedDF))
      .collect()

    assert(flattenedResults != null)
    assert(flattenedResults.length == 15)
    assert(flattenedResults.toSeq.head.get(1).toString.startsWith("47.6418"))
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(extractFields(df1), extractFields(df2))(eq)
  }

  override def testObjects(): Seq[TestObject[AddressGeocoder]] =
    Seq(new TestObject[AddressGeocoder](
      batchGeocodeAddresses,
      new FixedMiniBatchTransformer().setBatchSize(5).transform(df)))

  override def reader: MLReadable[_] = AddressGeocoder
}

class AzMapsSearchReverseAddressSuite extends TransformerFuzzing[ReverseAddressGeocoder] with AzureMapsKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (48.858561, 2.294911),
    (47.639765, -122.127896),
    (47.621028, -122.348170),
    (43.722990, 10.396695),
    (40.750958, -73.982336),
    (48.858561, 2.394911),
    (47.739765, -120.127896),
    (47.651028, -121.348170),
    (43.752990, 11.396695),
    (40.720958, -74.982336)
  ).toDF("lat", "lon")

  lazy val batchReverseGeocode: ReverseAddressGeocoder = new ReverseAddressGeocoder()
    .setSubscriptionKey(azureMapsKey)
    .setLatitudeCol("lat")
    .setLongitudeCol("lon")
    .setOutputCol("output")
    .setErrorCol("errors")

  def extractFields(df: DataFrame): DataFrame = {
    df.select(
      col("lat"),
      col("lon"),
      col("output.response.addresses").getItem(0).getField("address")
        .getField("freeformAddress").as("address"),
      col("output.response.addresses").getItem(0).getField("address")
        .getField("country").as("country"))
  }

  test("Usage with strange address responses"){
    val df = Seq(
      (45.32202, -75.83875),
      (47.62039, -122.34928),
      (45.37032, -75.70161),
      (45.3702793, -75.7037331),
      (45.28471, -75.73897),
      (48.85607, 2.29833)
    ).toDF("lat", "lon")

    val geocoder = new ReverseAddressGeocoder()
      .setSubscriptionKey(azureMapsKey)
      .setLatitudeCol("lat")
      .setLongitudeCol("lon")
      .setOutputCol("output")

    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df)

    assert(geocoder.transform(batchedDF).where(col("output").isNull).count() == 0)
  }

  test("Basic Batch Reverse Geocode Usage") {
    val batchedDF = batchReverseGeocode.transform(new FixedMiniBatchTransformer().setBatchSize(5)
      .transform(df.coalesce(1)))
    val flattenedResults = extractFields(new FlattenBatch().transform(batchedDF)).collect()

    assert(flattenedResults != null)
    assert(flattenedResults.length == 10)

  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(extractFields(df1), extractFields(df2))(eq)
  }

  override def testObjects(): Seq[TestObject[ReverseAddressGeocoder]] =
    Seq(new TestObject[ReverseAddressGeocoder](
      batchReverseGeocode,
      new FixedMiniBatchTransformer().setBatchSize(5).transform(df)))

  override def reader: MLReadable[_] = ReverseAddressGeocoder
}

// AzMapsPointInPolygonSuite was removed because the Azure Maps Spatial service
// was retired on September 30, 2025. The CheckPointInPolygon transformer now
// throws UnsupportedOperationException on transform().
// See: https://azure.microsoft.com/en-us/updates/v2/azure-maps-creator-services-retirement-on-30-september-2025
