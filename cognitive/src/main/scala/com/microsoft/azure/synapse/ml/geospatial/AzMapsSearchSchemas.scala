// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import spray.json.DefaultJsonProtocol.jsonFormat5

import java.util.Date

object Address extends SparkBindings[Address]

case class Address (
  // The building number on the street. DEPRECATED, use streetNumber instead.
  buildingNumber: Option[String],
  // The street name. DEPRECATED, use streetName instead.
  street: Option[String],
  // The name of the street being crossed.
  crossStreet: Option[String],
  // The building number on the street.
  streetNumber: Option[String],
  // The codes used to unambiguously identify the street
  routeNumbers: Option[Seq[Integer]],
  // The street name.
  streetName: Option[String],
  // The street name and number.
  streetNameAndNumber: Option[String],
  // City / Town
  municipality: Option[String],
  // Sub / Super City
  municipalitySubdivision: Option[String],
  // Named Area
  countryTertiarySubdivision: Option[String],
  // County
  countrySecondarySubdivision: Option[String],
  // State or Province
  countrySubdivision: Option[String],
  // Postal Code / Zip Code
  postalCode: Option[String],
  // Extended postal code (availability is dependent on the region).
  extendedPostalCode: Option[String],
  // Country (Note: This is a two-letter code, not a country name.)
  countryCode: Option[String],
  // Country name
  country: Option[String],
  // ISO alpha-3 country code
  countryCodeISO3: Option[String],
  // An address line formatted according to the formatting rules of a Result's country of origin,
  // or in the case of a country, its full country name.
  freeformAddress: Option[String],
  // The full name of a first level of country administrative hierarchy.
  // This field appears only in case countrySubdivision is presented in an abbreviated form.
  // Only supported for USA, Canada, and Great Britain.
  countrySubdivisionName: Option[String],
  // An address component which represents the name of a geographic area or locality that groups a number of
  // addressable objects for addressing purposes, without being an administrative unit.
  // This field is used to build the `freeformAddress` property.
  localName: Option[String],
  // Bounding box coordinates.
  boundingBox: Option[BoundingBox])

case class AddressRanges (
  // Address range on the left side of the street.
  rangeLeft: Option[String],
  // Address range on the right side of the street.
  rangeRight: Option[String],
  from: Option[LatLongPairAbbreviated],
  to: Option[LatLongPairAbbreviated])

case class BrandName (
  // Name of the brand
  name: Option[String])

case class BoundingBox (
  // top-left
  topLeftPoint: Option[LatLongPairAbbreviated],
  // bottom-right
  btmRightPoint: Option[LatLongPairAbbreviated])

case class Classification (
  // Code property
  code: Option[String],
  // Names array
  names: Option[Seq[ClassificationName]])

case class ClassificationName (
  // Name Locale property
  nameLocale: Option[String],
  // Name property
  name: Option[String])

case class DataSources (
  geometry: Option[Geometry])

case class EntryPoint (
  // The type of entry point. Value can be either _main_ or _minor_.
  `type`: Option[String],
  position: Option[LatLongPairAbbreviated])

case class ErrorDetail (
  // The error code.
  code: Option[String],
  // The error message.
  message: Option[String],
  // The error target.
  target: Option[String],
  // The error details.
  details: Option[Seq[String]],
  // The error additional info.
  additionalInfo: Option[Seq[ErrorAdditionalInfo]])

case class ErrorAdditionalInfo (
  // The additional info type.
  `type`: Option[String],
  // The additional info.
  info: Option[String])

case class Geometry (
  // Pass this as geometryId to the
  // [Get Search Polygon](https://docs.microsoft.com/rest/api/maps/search/getsearchpolygon) API to fetch
  // geometry information for this result.
  id: Option[String])

case class LatLongPair (
  // Latitude property
  latitude: Option[Double],
  // Longitude property
  longitude: Option[Double])

case class LatLongPairAbbreviated (
  // Latitude property
  lat: Option[Double],
  // Longitude property
  lon: Option[Double])

object LongRunningOperationResult extends SparkBindings[LongRunningOperationResult]
case class LongRunningOperationResult (
  // The Id for this long-running operation.
  operationId: Option[String],
  // The status state of the request.
  status: Option[String],
  // resource location
  resourceLocation: Option[String],
  // The created timestamp.
  created: Option[String],
  error: Option[ErrorDetail],
  warning: Option[ErrorDetail])


case class OperatingHours (
  // Value used in the request: none or \"nextSevenDays\"
  mode: Option[String],
  // List of time ranges for the next 7 days
  timeRanges: Option[Seq[OperatingHoursTimeRange]])

case class OperatingHoursTime (
  // Represents current calendar date in POI time zone, e.g. \"2019-02-07\".
  date: Option[String],
  // Hours are in the 24 hour format in the local time of a POI; possible values are 0 - 23.
  hour: Option[Integer],
  // Minutes are in the local time of a POI; possible values are 0 - 59.
  minute: Option[Integer])

case class OperatingHoursTimeRange (
 // The point in the next 7 days range when a given POI is being opened, or the beginning of the range
 // if it was opened before the range.
 startTime: Option[OperatingHoursTime],
 // The point in the next 7 days range when a given POI is being closed, or the beginning of the range
 // if it was closed before the range.
 endTime: Option[OperatingHoursTime])

case class PointOfInterest (
 // Name of the POI property
 name: Option[String],
 // Telephone number property
 phone: Option[String],
 // Website URL property
 url: Option[String],
 // The list of the most specific POI categories
 categorySet: Option[Seq[PointOfInterestCategorySet]],
 // Classification array
 classifications: Option[Seq[Classification]],
 // Brands array. The name of the brand for the POI being returned.
 brands: Option[Seq[BrandName]],
 openingHours: Option[OperatingHours])

case class PointOfInterestCategorySet (
  // Category ID
  id: Option[Integer])

object ReverseSearchAddressResult extends SparkBindings[ReverseSearchAddressResult]
case class ReverseSearchAddressResult (
  // The error object.
  error: Option[ErrorDetail],
  // Summary object for a Search Address Reverse response
  summary: Option[SearchSummary],
  // Addresses array
  addresses: Option[Seq[ReverseSearchAddressResultItem]])

case class ReverseSearchAddressResultItem (
  address: Option[Address],
  // Position property in the form of \"{latitude},{longitude}\"
  position: Option[String],
  roadUse: Option[Seq[String]],
  // Information on the type of match.  One of:   * AddressPoint   * HouseNumberRange   * Street
  matchType: Option[String])

object ReverseSearchAddressBatchResult extends SparkBindings[ReverseSearchAddressBatchResult]
case class ReverseSearchAddressBatchResult (
  summary: Option[SearchBatchSummary],
  batchItems: Option[Seq[ReverseSearchAddressBatchItem]])

object ReverseSearchAddressBatchItem extends SparkBindings[ReverseSearchAddressBatchItem]
case class ReverseSearchAddressBatchItem (
  // HTTP request status code.
  statusCode: Option[Integer],
  // The result of the query.
  response: Option[ReverseSearchAddressResult])

case class SearchBatchSummary(
  successfulRequests: Option[Integer],
  totalRequests: Option[Integer]
)

object SearchAddressBatchProcessResult extends SparkBindings[SearchAddressBatchProcessResult]
case class SearchAddressBatchProcessResult (
  // Summary of the results for the batch request
  summary: Option[SearchBatchSummary],
  // Array containing the batch results.
  batchItems: Option[Seq[SearchAddressBatchItem]])

object SearchAddressBatchItem extends SparkBindings[SearchAddressBatchItem]
case class SearchAddressBatchItem (
  // HTTP request status code.
  statusCode: Option[Integer],
  // The result of the query.
  response: Option[SearchAddressResult])

object SearchAddressResult extends SparkBindings[SearchAddressResult]
case class SearchAddressResult (
   // The error object.
   error: Option[ErrorDetail],
  // Summary object for a Search API response
  summary: SearchSummary,
  // A list of Search API results.
  results: Option[Seq[SearchAddressResultItem]])

case class SearchSummary (
  // The query parameter that was used to produce these search results.
  query: Option[String],
  // The type of query being returned: NEARBY or NON_NEAR.
  queryType: Option[String],
  // Time spent resolving the query, in milliseconds.
  queryTime: Option[Integer],
  // Number of results in the response.
  numResults: Option[Integer],
  // Maximum number of responses that will be returned
  limit: Option[Integer],
  // The starting offset of the returned Results within the full Result set.
  offset: Option[Integer],
  // The total number of Results found.
  totalResults: Option[Integer],
  // The maximum fuzzy level required to provide Results.
  fuzzyLevel: Option[Integer],
  // Indication when the internal search engine has applied a geospatial bias to improve the ranking of results.
  // In  some methods, this can be affected by setting the lat and lon parameters where available.
  // In other cases it is  purely internal.
  geoBias: Option[LatLongPairAbbreviated])

case class SearchAddressResultItem (
 // One of: * POI * Street * Geography * Point Address * Address Range * Cross Street
 `type`: Option[String],
 // Id property
 id: Option[String],
 score: Option[Double],
 dist: Option[Double],
 // Information about the original data source of the Result. Used for support requests.
 info: Option[String],
 entityType: Option[String],
 poi: Option[PointOfInterest],
 address: Option[Address],
 position: Option[LatLongPairAbbreviated],
 viewport: Option[Viewport],
 // Array of EntryPoints. Those describe the types of entrances available at the location.
 // The type can be \"main\" for main entrances such as a front door, or a lobby, and \"minor\",
 // for side and back doors.
 entryPoints: Option[Seq[EntryPoint]],
 addressRanges: Option[AddressRanges],
 // Optional section. Reference geometry id for use with the
 // [Get Search Polygon](https://docs.microsoft.com/rest/api/maps/search/getsearchpolygon) API.
 dataSources: Option[DataSources],
 // Information on the type of match.  One of:   * AddressPoint   * HouseNumberRange   * Street
 matchType: Option[String],
 // Detour time in seconds. Only returned for calls to the Search Along Route API.
 detourTime: Option[Integer])

case class Viewport (
  topLeftPoint: Option[LatLongPairAbbreviated],
  btmRightPoint: Option[LatLongPairAbbreviated])

object AzureMapsJsonProtocol extends DefaultJsonProtocol {
  implicit val ErrorAdditionalInfoFormat: RootJsonFormat[ErrorAdditionalInfo] = jsonFormat2(ErrorAdditionalInfo.apply)
  implicit val ErrorDetailFormat: RootJsonFormat[ErrorDetail] = jsonFormat5(ErrorDetail.apply)
  implicit val LRORFormat: RootJsonFormat[LongRunningOperationResult] = jsonFormat6(LongRunningOperationResult.apply)
}
