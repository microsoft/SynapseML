// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.geospatial

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType

import java.net.{URI, URLDecoder}

private[geospatial] class TestableAddressGeocoder extends AddressGeocoder {
  def buildRequest(row: Row): Option[HttpPost] =
    inputFunc(row).map(_.asInstanceOf[HttpPost])
}

private[geospatial] class TestableReverseAddressGeocoder extends ReverseAddressGeocoder {
  def buildRequest(row: Row): Option[HttpPost] =
    inputFunc(row).map(_.asInstanceOf[HttpPost])
}

private[geospatial] class TestableCheckPointInPolygon extends CheckPointInPolygon {
  def buildRequest(row: Row): Option[HttpGet] =
    inputFunc(row).map(_.asInstanceOf[HttpGet])
}

class GeospatialCoreSuite extends TestBase {

  import spark.implicits._

  private def toQueryMap(uri: URI): Map[String, String] = {
    Option(uri.getRawQuery).toSeq.flatMap(_.split("&")).map { kv =>
      val pair = kv.split("=", 2)
      val key = URLDecoder.decode(pair(0), "UTF-8")
      val value = if (pair.length > 1) URLDecoder.decode(pair(1), "UTF-8") else ""
      key -> value
    }.toMap
  }

  test("address geocoder builds deterministic request payload and query params") {
    val request = new TestableAddressGeocoder()
      .setSubscriptionKey("fake-key")
      .setAddress(Seq("One Microsoft Way, Redmond", "400 Broad St, Seattle"))
      .buildRequest(Row.empty)
      .get

    val query = toQueryMap(request.getURI)
    assert(request.getURI.getPath.endsWith("/search/address/batch/json"))
    assert(query("api-version") == "1.0")
    assert(query("subscription-key") == "fake-key")
    assert(request.getFirstHeader("Content-Type").getValue == "application/json")

    val payload = EntityUtils.toString(request.getEntity, "UTF-8")
    assert(payload.contains("?query=One+Microsoft+Way%2C+Redmond&limit=1"))
    assert(payload.contains("?query=400+Broad+St%2C+Seattle&limit=1"))
  }

  test("reverse geocoder builds deterministic request payload and query params") {
    val request = new TestableReverseAddressGeocoder()
      .setSubscriptionKey("fake-key")
      .setLatitude(Seq(48.858561, 47.639765))
      .setLongitude(Seq(2.294911, -122.127896))
      .buildRequest(Row.empty)
      .get

    val query = toQueryMap(request.getURI)
    assert(request.getURI.getPath.endsWith("/search/address/reverse/batch/json"))
    assert(query("api-version") == "1.0")
    assert(query("subscription-key") == "fake-key")
    assert(request.getFirstHeader("Content-Type").getValue == "application/json")

    val payload = EntityUtils.toString(request.getEntity, "UTF-8")
    assert(payload.contains("?query=48.858561,2.294911&limit=1"))
    assert(payload.contains("?query=47.639765,-122.127896&limit=1"))
  }

  test("address and reverse schema behavior is deterministic") {
    val addressInput = Seq(Seq("One Microsoft Way, Redmond")).toDF("address")
    val addressSchema = new AddressGeocoder()
      .setAddressCol("address")
      .setOutputCol("output")
      .setErrorCol("addressError")
      .transformSchema(addressInput.schema)
    assert(addressSchema.fieldNames.toSet == Set("address", "output", "addressError"))
    assert(addressSchema("output").dataType == ArrayType(SearchAddressBatchItem.schema))

    val reverseInput = Seq((Seq(47.6418), Seq(-122.1275))).toDF("latitude", "longitude")
    val reverseSchema = new ReverseAddressGeocoder()
      .setLatitudeCol("latitude")
      .setLongitudeCol("longitude")
      .setOutputCol("output")
      .setErrorCol("reverseError")
      .transformSchema(reverseInput.schema)
    assert(reverseSchema.fieldNames.toSet == Set("latitude", "longitude", "output", "reverseError"))
    assert(reverseSchema("output").dataType == ArrayType(ReverseSearchAddressBatchItem.schema))
  }

  test("geospatial transformers validate missing input columns locally") {
    val addressInput = Seq(Seq("One Microsoft Way, Redmond")).toDF("address")
    val addressError = intercept[AssertionError] {
      new AddressGeocoder().setAddressCol("missingAddress").transformSchema(addressInput.schema)
    }
    assert(addressError.getMessage.contains("Could not find dynamic columns"))
    assert(addressError.getMessage.contains("missingAddress"))

    val reverseInput = Seq((Seq(47.6418), Seq(-122.1275))).toDF("latitude", "longitude")
    val reverseError = intercept[AssertionError] {
      new ReverseAddressGeocoder()
        .setLatitudeCol("latitude")
        .setLongitudeCol("missingLongitude")
        .transformSchema(reverseInput.schema)
    }
    assert(reverseError.getMessage.contains("Could not find dynamic columns"))
    assert(reverseError.getMessage.contains("missingLongitude"))
  }

  test("checkpoint helper logic, schema behavior, and retired transform are deterministic") {
    val transformer = new TestableCheckPointInPolygon()
      .setSubscriptionKey("fake-key")
      .setGeography("us")
      .setUserDataIdentifier("udid-1")
      .setLatitude(47.6418)
      .setLongitude(-122.1275)

    assert(transformer.getUrl == "https://us.atlas.microsoft.com/spatial/pointInPolygon/json")
    assert(transformer.getLatitude == Seq(47.6418))
    assert(transformer.getLongitude == Seq(-122.1275))
    assert(transformer.getUserDataIdentifier == "udid-1")

    val request = transformer.buildRequest(Row.empty).get
    val query = toQueryMap(request.getURI)
    assert(request.getURI.getPath.endsWith("/spatial/pointInPolygon/json"))
    assert(query("api-version") == "1.0")
    assert(query("subscription-key") == "fake-key")
    assert(query("udid") == "udid-1")
    assert(query("lat").contains("47.6418"))
    assert(query("lon").contains("-122.1275"))

    val input = Seq((Seq(47.6418), Seq(-122.1275), "udid-1")).toDF("latitude", "longitude", "udid")
    val schema = new CheckPointInPolygon()
      .setLatitudeCol("latitude")
      .setLongitudeCol("longitude")
      .setUserDataIdentifierCol("udid")
      .setOutputCol("pointInPolygon")
      .setErrorCol("pointInPolygonError")
      .transformSchema(input.schema)
    assert(schema.fieldNames.toSet == Set("latitude", "longitude", "udid", "pointInPolygon", "pointInPolygonError"))
    assert(schema("pointInPolygon").dataType == PointInPolygonProcessResult.schema)

    val missingColumnError = intercept[AssertionError] {
      new CheckPointInPolygon()
        .setLatitudeCol("latitude")
        .setLongitudeCol("missingLongitude")
        .setUserDataIdentifierCol("udid")
        .transformSchema(input.schema)
    }
    assert(missingColumnError.getMessage.contains("Could not find dynamic columns"))
    assert(missingColumnError.getMessage.contains("missingLongitude"))

    val retiredError = intercept[UnsupportedOperationException] {
      transformer.transform(input)
    }
    assert(retiredError.getMessage.contains("retired on September 30, 2025"))
  }
}
