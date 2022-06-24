// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.spark

import org.apache.spark.ml.{Estimator, Model, NamespaceInjections, Transformer}
import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions
import scala.reflect.ClassTag

class DataFrameSugars(val df: DataFrame) {

  def mlTransform[T <: Transformer](t: T): DataFrame =
    t.transform(df)

  def mlTransform[T <: Transformer: ClassTag](ts: T*): DataFrame =
    NamespaceInjections.pipelineModel(ts.toArray).transform(df)

  def mlFit[E <: Estimator[_ <: Model[_]]](e: E): Model[_] = e.fit(df)

}

object FluentAPI {

  implicit def toSugaredDF(df: DataFrame): DataFrameSugars = new DataFrameSugars(df)

  implicit def fromSugaredDF(df: DataFrameSugars): DataFrame = df.df

}
