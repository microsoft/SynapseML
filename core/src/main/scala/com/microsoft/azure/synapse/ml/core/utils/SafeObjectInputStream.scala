// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import java.io.{InputStream, InvalidClassException, ObjectStreamClass}

/** An ObjectInputStream that restricts deserialization to an allowlist of class name prefixes.
  *
  * This mitigates Java deserialization attacks (CWE-502) by rejecting any class
  * whose fully-qualified name does not start with one of the allowed prefixes.
  * It also inherits the context-classloader resolution from [[ContextObjectInputStream]].
  *
  * @param input            the underlying input stream
  * @param allowedPrefixes  set of class name prefixes that are permitted for deserialization
  */
class SafeObjectInputStream(
    input: InputStream,
    allowedPrefixes: Set[String]
) extends ContextObjectInputStream(input) {

  protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    val className = desc.getName
    if (!allowedPrefixes.exists(prefix => className.startsWith(prefix))) {
      throw new InvalidClassException(
        className,
        "Deserialization of this class is not allowed. " +
          "Only classes with approved package prefixes may be deserialized."
      )
    }
    super.resolveClass(desc)
  }
}

object SafeObjectInputStream {

  /** Default allowlist suitable for deserializing SynapseML nn package objects
    * (BallTree, ConditionalBallTree, and their object graphs).
    */
  val DefaultNNAllowedPrefixes: Set[String] = Set(
    "com.microsoft.azure.synapse.ml.nn.",
    "breeze.",
    "scala.",
    "java.lang.",
    "java.util.",
    "java.io.",
    "java.math.",
    "[" // Java array type descriptors
  )

  /** Broader allowlist suitable for general SynapseML model deserialization
    * via [[org.apache.spark.ml.Serializer]]. Covers all SynapseML modules,
    * Spark ML types, and common numeric/ML library dependencies while
    * excluding packages commonly exploited in deserialization attacks.
    */
  val DefaultAllowedPrefixes: Set[String] = Set(
    "com.microsoft.azure.synapse.ml.",
    "org.apache.spark.",
    "breeze.",
    "scala.",
    "java.lang.",
    "java.util.",
    "java.io.",
    "java.math.",
    "java.sql.",
    "java.time.",
    "org.apache.hadoop.io.",
    "com.microsoft.ml.lightgbm.",
    "org.vowpalwabbit.",
    "[" // Java array type descriptors
  )
}
