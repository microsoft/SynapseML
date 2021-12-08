// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import java.util.Date

object Address extends SparkBindings[Address]

case class Address (
  // The building number on the street. DEPRECATED, use streetNumber instead.
  buildingNumber: Option[String] = None,
  // The street name. DEPRECATED, use streetName instead.
  street: Option[String] = None,
  // The name of the street being crossed.
  crossStreet: Option[String] = None,
  // The building number on the street.
  streetNumber: Option[String] = None,
  // The codes used to unambiguously identify the street
  routeNumbers: Option[Seq[Integer]] = None,
  // The street name.
  streetName: Option[String] = None,
  // The street name and number.
  streetNameAndNumber: Option[String] = None,
  // City / Town
  municipality: Option[String] = None,
  // Sub / Super City
  municipalitySubdivision: Option[String] = None,
  // Named Area
  countryTertiarySubdivision: Option[String] = None,
  // County
  countrySecondarySubdivision: Option[String] = None,
  // State or Province
  countrySubdivision: Option[String] = None,
  // Postal Code / Zip Code
  postalCode: Option[String] = None,
  // Extended postal code (availability is dependent on the region).
  extendedPostalCode: Option[String] = None,
  // Country (Note: This is a two-letter code, not a country name.)
  countryCode: Option[String] = None,
  // Country name
  country: Option[String] = None,
  // ISO alpha-3 country code
  countryCodeISO3: Option[String] = None,
  // An address line formatted according to the formatting rules of a Result's country of origin,
  // or in the case of a country, its full country name.
  freeformAddress: Option[String] = None,
  // The full name of a first level of country administrative hierarchy.
  // This field appears only in case countrySubdivision is presented in an abbreviated form.
  // Only supported for USA, Canada, and Great Britain.
  countrySubdivisionName: Option[String] = None,
  // An address component which represents the name of a geographic area or locality that groups a number of
  // addressable objects for addressing purposes, without being an administrative unit.
  // This field is used to build the `freeformAddress` property.
  localName: Option[String] = None,
  // Bounding box coordinates.
  boundingBox: Option[BoundingBox] = None
)

case class AddressRanges (
  // Address range on the left side of the street.
  rangeLeft: Option[String] = None,
  // Address range on the right side of the street.
  rangeRight: Option[String] = None,
  from: Option[LatLongPairAbbreviated] = None,
  to: Option[LatLongPairAbbreviated] = None
)

case class BrandName (
  // Name of the brand
  name: Option[String] = None
)

case class BoundingBox (
  // top-left
  topLeftPoint: Option[LatLongPairAbbreviated] = None,
  // bottom-right
  btmRightPoint: Option[LatLongPairAbbreviated] = None
)

case class Classification (
  // Code property
  code: Option[String] = None,
  // Names array
  names: Option[Seq[ClassificationName]] = None
)

case class ClassificationName (
  // Name Locale property
  nameLocale: Option[String] = None,
  // Name property
  name: Option[String] = None
)


case class DataSources (
  geometry: Option[Geometry] = None
)

case class EntryPoint (
  // The type of entry point. Value can be either _main_ or _minor_.
  `type`: Option[String] = None,
  position: Option[LatLongPairAbbreviated] = None
)

case class ErrorDetail (
  // The error code.
  code: Option[String] = None,
  // The error message.
  message: Option[String] = None,
  // The error target.
  target: Option[String] = None,
  // The error details.
  details: Option[Seq[String]] = None,
  // The error additional info.
  additionalInfo: Option[Seq[ErrorAdditionalInfo]] = None
)


case class ErrorAdditionalInfo (
  // The additional info type.
  `type`: Option[String] = None,
  // The additional info.
  info: Option[String] = None
)

case class Geometry (
  // Pass this as geometryId to the
  // [Get Search Polygon](https://docs.microsoft.com/rest/api/maps/search/getsearchpolygon) API to fetch
  // geometry information for this result.
  id: Option[String] = None
)

case class LatLongPair (
  // Latitude property
  latitude: Option[Double] = None,
  // Longitude property
  longitude: Option[Double] = None
)

case class LatLongPairAbbreviated (
  // Latitude property
  lat: Option[Double] = None,
  // Longitude property
  lon: Option[Double] = None
)

object LongRunningOperationResult extends SparkBindings[LongRunningOperationResult]
case class LongRunningOperationResult (
  // The Id for this long-running operation.
  operationId: Option[String] = None,
  // The status state of the request.
  status: Option[String] = None,
  // The created timestamp.
  created: Option[Date] = None,
  error: Option[ErrorDetail] = None,
  warning: Option[ErrorDetail] = None
)


case class OperatingHours (
  // Value used in the request: none or \"nextSevenDays\"
  mode: Option[String] = None,
  // List of time ranges for the next 7 days
  timeRanges: Option[Seq[OperatingHoursTimeRange]] = None
)

case class OperatingHoursTime (
  // Represents current calendar date in POI time zone, e.g. \"2019-02-07\".
  date: Option[String] = None,
  // Hours are in the 24 hour format in the local time of a POI; possible values are 0 - 23.
  hour: Option[Integer] = None,
  // Minutes are in the local time of a POI; possible values are 0 - 59.
  minute: Option[Integer] = None
)

case class OperatingHoursTimeRange (
 // The point in the next 7 days range when a given POI is being opened, or the beginning of the range
 // if it was opened before the range.
 startTime: Option[OperatingHoursTime] = None,
 // The point in the next 7 days range when a given POI is being closed, or the beginning of the range
 // if it was closed before the range.
 endTime: Option[OperatingHoursTime] = None
)


case class PointOfInterest (
 // Name of the POI property
 name: Option[String] = None,
 // Telephone number property
 phone: Option[String] = None,
 // Website URL property
 url: Option[String] = None,
 // The list of the most specific POI categories
 categorySet: Option[Seq[PointOfInterestCategorySet]] = None,
 // Classification array
 classifications: Option[Seq[Classification]] = None,
 // Brands array. The name of the brand for the POI being returned.
 brands: Option[Seq[BrandName]] = None,
 openingHours: Option[OperatingHours] = None
)

case class PointOfInterestCategorySet (
  // Category ID
  id: Option[Integer] = None
)


object ReverseSearchAddressResult extends SparkBindings[ReverseSearchAddressResult]
case class ReverseSearchAddressResult (
  // The error object.
  error: Option[ErrorDetail] = None,
  // Summary object for a Search Address Reverse response
  summary: Option[SearchSummary] = None,
  // Addresses array
  addresses: Option[Seq[ReverseSearchAddressResultItem]] = None
)

case class ReverseSearchAddressResultItem (
  address: Option[Address] = None,
  // Position property in the form of \"{latitude},{longitude}\"
  position: Option[String] = None,
  roadUse: Option[Seq[String]] = None,
  // Information on the type of match.  One of:   * AddressPoint   * HouseNumberRange   * Street
  matchType: Option[String] = None
)

object ReverseSearchAddressBatchProcessResult extends SparkBindings[ReverseSearchAddressBatchProcessResult]
case class ReverseSearchAddressBatchProcessResult (
  summary: Option[SearchBatchSummary] = None,
  batchItems: Option[Seq[ReverseSearchAddressBatchItem]] = None
)

object ReverseSearchAddressBatchItem extends SparkBindings[ReverseSearchAddressBatchItem]
case class ReverseSearchAddressBatchItem (
  // HTTP request status code.
  statusCode: Option[Integer] = None,
  // The result of the query.
  response: Option[ReverseSearchAddressResult] = None
)

case class SearchBatchSummary(
  successfulRequests: Option[Integer] = None,
  totalRequests: Option[Integer] = None
)

object SearchAddressBatchProcessResult extends SparkBindings[SearchAddressBatchProcessResult]
case class SearchAddressBatchProcessResult (
  // Summary of the results for the batch request
  summary: Option[SearchBatchSummary] = None,
  // Array containing the batch results.
  batchItems: Option[Seq[SearchAddressBatchItem]] = None
)

object SearchAddressBatchItem extends SparkBindings[SearchAddressBatchItem]
case class SearchAddressBatchItem (
  // HTTP request status code.
  statusCode: Option[Integer] = None,
  // The result of the query.
  response: Option[SearchAddressResult] = None
)

object SearchAddressResult extends SparkBindings[SearchAddressResult]
case class SearchAddressResult (
   // The error object.
   error: Option[ErrorDetail] = None,
  // Summary object for a Search API response
  summary: SearchSummary,
  // A list of Search API results.
  results: Option[Seq[SearchAddressResultItem]] = None,
)

case class SearchSummary (
  // The query parameter that was used to produce these search results.
  query: Option[String] = None,
  // The type of query being returned: NEARBY or NON_NEAR.
  queryType: Option[String] = None,
  // Time spent resolving the query, in milliseconds.
  queryTime: Option[Integer] = None,
  // Number of results in the response.
  numResults: Option[Integer] = None,
  // Maximum number of responses that will be returned
  limit: Option[Integer] = None,
  // The starting offset of the returned Results within the full Result set.
  offset: Option[Integer] = None,
  // The total number of Results found.
  totalResults: Option[Integer] = None,
  // The maximum fuzzy level required to provide Results.
  fuzzyLevel: Option[Integer] = None,
  // Indication when the internal search engine has applied a geospatial bias to improve the ranking of results.
  // In  some methods, this can be affected by setting the lat and lon parameters where available.
  // In other cases it is  purely internal.
  geoBias: Option[LatLongPairAbbreviated] = None
)

case class SearchAddressResultItem (
 // One of: * POI * Street * Geography * Point Address * Address Range * Cross Street
 `type`: Option[String] = None,
 // Id property
 id: Option[String] = None,
 score: Option[Double] = None,
 dist: Option[Double] = None,
 // Information about the original data source of the Result. Used for support requests.
 info: Option[String] = None,
 entityType: Option[String] = None,
 poi: Option[PointOfInterest] = None,
 address: Option[Address] = None,
 position: Option[LatLongPairAbbreviated] = None,
 viewport: Option[Viewport] = None,
 // Array of EntryPoints. Those describe the types of entrances available at the location.
 // The type can be \"main\" for main entrances such as a front door, or a lobby, and \"minor\",
 // for side and back doors.
 entryPoints: Option[Seq[EntryPoint]] = None,
 addressRanges: Option[AddressRanges] = None,
 // Optional section. Reference geometry id for use with the
 // [Get Search Polygon](https://docs.microsoft.com/rest/api/maps/search/getsearchpolygon) API.
 dataSources: Option[DataSources] = None,
 // Information on the type of match.  One of:   * AddressPoint   * HouseNumberRange   * Street
 matchType: Option[String] = None,
 // Detour time in seconds. Only returned for calls to the Search Along Route API.
 detourTime: Option[Integer] = None
)

case class Viewport (
  topLeftPoint: Option[LatLongPairAbbreviated] = None,
  btmRightPoint: Option[LatLongPairAbbreviated] = None
)
