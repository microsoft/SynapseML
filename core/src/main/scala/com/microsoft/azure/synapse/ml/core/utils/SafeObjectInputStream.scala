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

  /** Extracts the component type name from a JVM array descriptor.
    * Primitive arrays (e.g. `[I`, `[D`) return None since they are always safe.
    * Object arrays (e.g. `[Lcom.example.Foo;`) return the fully-qualified class name.
    * Multi-dimensional arrays are unwrapped recursively (e.g. `[[Ljava.lang.String;`).
    */
  private def extractArrayComponentName(className: String): Option[String] = {
    val stripped = className.dropWhile(_ == '[')
    if (stripped.startsWith("L") && stripped.endsWith(";")) {
      Some(stripped.substring(1, stripped.length - 1))
    } else {
      None // primitive array (B, C, D, F, I, J, S, Z)
    }
  }

  private def isAllowed(className: String): Boolean = {
    allowedPrefixes.exists(prefix => className.startsWith(prefix))
  }

  protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    val className = desc.getName
    val allowed = if (className.startsWith("[")) {
      extractArrayComponentName(className) match {
        case Some(componentName) => isAllowed(componentName)
        case None => true // primitive arrays are always safe
      }
    } else {
      isAllowed(className)
    }

    if (!allowed) {
      throw new InvalidClassException(
        className,
        "Deserialization of this class is not allowed. " +
          "Only classes with approved package prefixes may be deserialized."
      )
    }
    super.resolveClass(desc)
  }

  /** Rejects dynamic proxy deserialization unless every interface is allowlisted.
    *
    * Dynamic proxies are a known deserialization attack vector (e.g. via
    * `java.lang.reflect.Proxy` with malicious `InvocationHandler` chains).
    * SynapseML model serialization does not use proxies, so this rejects
    * them by default while still validating interface names for safety.
    */
  protected override def resolveProxyClass(interfaces: Array[String]): Class[_] = {
    val disallowed = interfaces.filterNot(isAllowed)
    if (disallowed.nonEmpty) {
      throw new InvalidClassException(
        disallowed.mkString(", "),
        "Deserialization of dynamic proxy is not allowed. " +
          "Proxy interface(s) not in the approved allowlist."
      )
    }
    super.resolveProxyClass(interfaces)
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
    "java.math."
  )
}
