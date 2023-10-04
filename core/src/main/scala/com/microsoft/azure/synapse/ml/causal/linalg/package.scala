package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.sql.Dataset

package object linalg {
  type DVector = Dataset[VectorEntry]
  type DMatrix = Dataset[MatrixEntry]
}
