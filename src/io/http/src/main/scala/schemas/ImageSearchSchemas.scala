// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.core.schema.SparkBindings
import org.apache.spark.sql.Row
import spray.json.RootJsonFormat

// Bing Schema
/*case class BingImagesResponse(`_type`: String,
                              id: String,
                              isFamilyFriendly: Boolean,
                              nextOffset: Int,
                              pivotSuggestions: BingPivot,
                              queryExpansions: BingQuery,
                              readLink: String,
                              similarTerms: BingQuery,
                              totalEstimatedMatches: Long,
                              value: Array[BingImage],
                              webSearchUrl: String)*/
//What the web says^ is WRONG!

case class BingImagesResponse(_type: String,
                              instrumentation: BingInstrumentation,
                              webSearchUrl: String,
                              totalEstimatedMatches: Int,
                              nextOffset: Int,
                              value: Seq[BingImage],
                              pivotSuggestions: Seq[BingPivot],
                              queryExpansions: Seq[BingQuery],
                              relatedSearches: Seq[BingQuery])

object BingImagesResponse extends SparkBindings[BingImagesResponse]

case class BingInstrumentation(_type: String)

case class BingPivot(pivot: String, suggestions: Seq[BingQuery])

case class BingQuery(displayText: String,
                     searchLink: String,
                     text: String,
                     thumbnail: BingThumbnail,
                     webSearchUrl: String)

case class BingThumbnail(thumbnailUrl: String)

case class BingImage(accentColor: String,
                     contentSize: String,
                     contentUrl: String,
                     datePublished: String,
                     encodingFormat: String,
                     height: Int,
                     hostPageDisplayUrl: String,
                     hostPageUrl: String,
                     id: String,
                     imageId: String,
                     imageInsightsToken: String,
                     insightsMetadata: String, // making this BingInsightsMetadata is circular
                     name: String,
                     thumbnail: BingMediaSize,
                     thumbnailUrl: String,
                     webSearchUrl: String,
                     width: Int)

object BingImage extends SparkBindings[BingImage]

case class BingMediaSize(height: Int, width: Int)

/*
case class BingInsightsMetadata(aggregateOffer: BingOffer,
                                recipeSourcesCount: Int,
                                shoppingSourcesCount: Int)

case class BingOffer(aggregateRating: BingAggregateRating,
                     availability: String,
                     description: String,
                     lastUpdated: String,
                     lowPrice: Float,
                     name: String,
                     offerCount: Int,
                     price: Float,
                     priceCurrency: String,
                     seller: BingOrganization,
                     url: String)

case class BingAggregateRating(bestRating: Float,
                               ratingValue: Float,
                               reviewCount: Int,
                               text: String)

case class BingOrganization(image: BingImage,
                            name: String)
*/
