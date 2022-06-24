// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings

object CompletionResponse extends SparkBindings[CompletionResponse]

case class CompletionResponse(id: String,
                              `object`: String,
                              created: String,
                              model: String,
                              choices: Seq[OpenAIChoice])

case class OpenAIChoice(text: String,
                        index: Long,
                        logprobs: Option[OpenAILogProbs],
                        finish_reason: String)

case class OpenAILogProbs(tokens: Seq[String],
                          token_logprobs: Seq[Double],
                          top_logprobs: Seq[Map[String, Double]],
                          text_offset: Seq[Long])
