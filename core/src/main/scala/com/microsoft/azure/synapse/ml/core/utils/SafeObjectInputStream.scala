// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import java.io.{InputStream, InvalidClassException, ObjectStreamClass}

final class DeserializationClassRejectedException(className: String, reason: String)
  extends InvalidClassException(className, reason)

final case class DeserializationClassFilter(
    allowedPrefixes: Set[String] = Set.empty,
    allowedClasses: Set[String] = Set.empty) {

  require(!allowedPrefixes.contains(""), "Deserialization class prefixes cannot contain an empty prefix")

  private[utils] def allows(className: String): Boolean = {
    allowedClasses.contains(className) || allowedPrefixes.exists(className.startsWith)
  }
}

/** An ObjectInputStream that restricts deserialization to an allowlist of class names and prefixes.
  *
  * This mitigates Java deserialization attacks (CWE-502) by rejecting any class
  * whose fully-qualified name is not allowed by the configured exact-name or prefix policy.
  * It also inherits the context-classloader resolution from [[ContextObjectInputStream]].
  *
  * @param input        the underlying input stream
  * @param classFilter  exact class names and class-name prefixes permitted for deserialization
  */
class SafeObjectInputStream(
    input: InputStream,
    classFilter: DeserializationClassFilter
) extends ContextObjectInputStream(input) {

  // SerializedLambda stores implementation classes as strings, so resolveClass cannot validate them.
  // ModuleSerializationProxy stores its target as Class, whose descriptor is validated by resolveClass.
  private val alwaysRejectedClasses = Set(
    "java.lang.invoke.SerializedLambda"
  )

  def this(input: InputStream, allowedPrefixes: Set[String]) = {
    this(input, DeserializationClassFilter(allowedPrefixes = allowedPrefixes))
  }

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
    !alwaysRejectedClasses.contains(className) && classFilter.allows(className)
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
      throw new DeserializationClassRejectedException(
        className,
        "Deserialization of this class is not allowed. " +
          "Only classes approved by the configured exact-name or prefix policy may be deserialized."
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
      throw new DeserializationClassRejectedException(
        disallowed.mkString(", "),
        "Deserialization of dynamic proxy is not allowed. " +
          "Proxy interface(s) not in the approved allowlist."
      )
    }
    super.resolveProxyClass(interfaces)
  }
}

object SafeObjectInputStream {

  val CommonDataAllowedPrefixes: Set[String] = Set(
    "java.lang.",
    "java.math.",
    "java.util.",
    "scala."
  )

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
