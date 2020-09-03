// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import scala.collection.mutable

trait ElementFeaturizer[E] {
  def featurize(idx: Int,
                value: E,
                indices: mutable.ArrayBuilder[Int],
                values: mutable.ArrayBuilder[Double]): Unit
}
