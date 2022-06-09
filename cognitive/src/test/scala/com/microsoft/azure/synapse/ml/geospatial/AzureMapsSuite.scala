// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.cognitive.URLEncodingUtils
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.geospatial.AzureMapsJsonProtocol._
import com.microsoft.azure.synapse.ml.io.http.{HeaderValues, RESTHelpers}
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch}
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalactic.Equality
import spray.json._

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
    assert(flattenedResults.toSeq.head.get(1) == 47.64016)
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

class AzMapsPointInPolygonSuite extends TransformerFuzzing[CheckPointInPolygon] with AzureMapsKey {

  import spark.implicits._

  var udid = ""

  lazy val df: DataFrame = Seq(
    (48.858561, 2.294911),
    (47.639765, -122.127896),
    (47.621028, -122.348170),
    (47.734012, -122.102737)
  ).toDF("lat", "lon")

  def extractFields(df: DataFrame): DataFrame = {
    df.select(
      col("lat"),
      col("lon"),
      col("output.result.pointInPolygons").as("Point In Polygon"),
      col("output.result.intersectingGeometries").as("Intersecting Polygons"))
  }

  test("Point in Polygon: Basic Usage") {
    println(s"Using $udid as the user-data identifier for these tests")
    pointInPolygonCheck.setUserDataIdentifier(udid)
    val result = extractFields(pointInPolygonCheck.transform(df)).collect()
    assert(result != null)
    assert(result.toSeq.head.get(2) === false)
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(extractFields(df1), extractFields(df2))(eq)
  }

  lazy val pointInPolygonCheck: CheckPointInPolygon = new CheckPointInPolygon()
    .setSubscriptionKey(azureMapsKey)
    .setGeography("us")
    .setLatitudeCol("lat")
    .setLongitudeCol("lon")
    .setOutputCol("output")
    .setUserDataIdentifier(udid)
    .setErrorCol("errors")

  override def afterAll(): Unit = {
    val queryParams = URLEncodingUtils.format(Map("api-version" -> "1.0",
      "subscription-key" -> azureMapsKey))
    tryWithRetries() { () =>
      val deleteRequest = new HttpDelete(new URI("https://us.atlas.microsoft.com/mapData/"
        + udid + "?" + queryParams))
      deleteRequest.setHeader("User-Agent",
        s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
      RESTHelpers.safeSend(deleteRequest, expectedCodes = Set(204))
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Setup a polygon to use
    val testPolygon =
      """
        |{ "type": "FeatureCollection", "features": [
        |    {
        |      "type": "Feature",
        |      "properties": { "geometryId": "test_geometry_id" },
        |      "geometry": {"type": "Polygon", "coordinates": [[[-122.14290618896484,47.67856488312544],
        |      [-122.03956604003906,47.67856488312544],[-122.03956604003906,47.7483271435476],
        |      [-122.14290618896484,47.7483271435476],[-122.14290618896484,47.67856488312544]]]} } ] }
        |""".stripMargin
    val queryParams = URLEncodingUtils.format(Map("api-version" -> "1.0", "subscription-key" -> azureMapsKey))
    val createRequest = new HttpPost(new URI("https://us.atlas.microsoft.com/mapData/upload?" + queryParams +
      "&dataFormat=geojson"))
    createRequest.setHeader("Content-Type", "application/json")
    createRequest.setEntity(new StringEntity(testPolygon))
    val response = RESTHelpers.safeSend(createRequest, expectedCodes = Set(202))
    val locationUrl = response.getFirstHeader("location").getValue
    assert(locationUrl != null)

    tryWithRetries(Array(3000, 5000, 10000, 20000, 30000)) { () =>
      val getLongRunningResult = new HttpGet(new URI(locationUrl + "&" + queryParams))
      val resourceLocation = RESTHelpers.sendAndParseJson(getLongRunningResult, expectedCodes = Set(201))
        .convertTo[LongRunningOperationResult]
        .resourceLocation.mkString
      udid = resourceLocation.split(Array('/', '?', '&'))(5)
    }
  }

  override def testObjects(): Seq[TestObject[CheckPointInPolygon]] =
    Seq(new TestObject[CheckPointInPolygon](pointInPolygonCheck, df))

  override def reader: MLReadable[_] = CheckPointInPolygon
}
