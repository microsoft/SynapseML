// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

object FileFormat extends Enumeration {
  type FileFormat = Value
  val Csv = Value("csv")
  val Tsv = Value("tsv")
  val Json = Value("json")
  val Parquet = Value("parquet")
}
