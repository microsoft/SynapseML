// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

object TokenLibrary {
  private val TokenLibraryClass = "com.microsoft.azure.trident.tokenlibrary.TokenLibrary$"
  private val InMemoryCacheClasses = Seq(
    "com.microsoft.azure.trident.tokenlibrary.InMemoryCacheClient$",
    "com.microsoft.azure.trident.tokenlibrary.cache.InMemoryCacheClient$",
    "com.microsoft.fabric.tokenlibrary.InMemoryCacheClient$",
    "com.microsoft.fabric.tokenlibrary.cache.InMemoryCacheClient$")
  private val NfsCacheClasses = Seq(
    "com.microsoft.azure.trident.tokenlibrary.NFSCache$",
    "com.microsoft.azure.trident.tokenlibrary.cache.NFSCache$",
    "com.microsoft.fabric.tokenlibrary.NFSCache$",
    "com.microsoft.fabric.tokenlibrary.cache.NFSCache$")
  private val SparkTokenVersion = 2
  private val SparkWorkloadType = "SparkCore"

  def getAccessToken: String = {
    val objectName = "com.microsoft.azure.trident.tokenlibrary.TokenLibrary"
    val mirror = currentMirror
    val module = mirror.staticModule(objectName)
    val obj = mirror.reflectModule(module).instance
    val objType = mirror.reflect(obj).symbol.toType
    val methodName = "getAccessToken"
    val methodSymbols = objType.decl(TermName(methodName)).asTerm.alternatives
    val argType = typeOf[String]
    val selectedMethodSymbol = methodSymbols.find { m =>
      m.asMethod.paramLists match {
        case List(List(param)) => param.typeSignature =:= argType
        case _ => false
      }
    }.getOrElse(throw new NoSuchMethodException(s"Method $methodName with argument type $argType not found"))
    val methodMirror = mirror.reflect(obj).reflectMethod(selectedMethodSymbol.asMethod)
    methodMirror("ml").asInstanceOf[String]
  }

  def getSparkMwcToken(workspaceId: String, artifactId: String): String = {
    val objectName = "com.microsoft.azure.trident.tokenlibrary.TokenLibrary"
    val mirror = currentMirror
    val module = mirror.staticModule(objectName)
    val obj = mirror.reflectModule(module).instance
    val objType = mirror.reflect(obj).symbol.toType
    val methodName = "getMwcToken"
    val methodSymbols = objType.decl(TermName(methodName)).asTerm.alternatives
    val argTypes = List(typeOf[String], typeOf[String], typeOf[Integer], typeOf[String])
    val selectedMethodSymbol = methodSymbols.find { m =>
      m.asMethod.paramLists.flatten.map(_.typeSignature).zip(argTypes).forall { case (a, b) => a =:= b }
    }.getOrElse(throw new NoSuchMethodException(s"Method $methodName with argument type not found"))
    val methodMirror = mirror.reflect(obj).reflectMethod(selectedMethodSymbol.asMethod)
    methodMirror(workspaceId, artifactId, SparkTokenVersion, SparkWorkloadType)
      .asInstanceOf[String]
  }

  private def objectMethod(classNames: Seq[String],
                           methodName: String,
                           parameterCount: Int): Option[(AnyRef, java.lang.reflect.Method)] = {
    classNames.iterator.flatMap { className =>
      try {
        val cls = Class.forName(className)
        val module = cls.getField("MODULE$").get(null).asInstanceOf[AnyRef] //scalastyle:ignore null
        (cls.getMethods ++ cls.getDeclaredMethods)
          .find(method => method.getName == methodName && method.getParameterCount == parameterCount)
          .map { method =>
            method.setAccessible(true)
            module -> method
          }
      } catch {
        case _: ClassNotFoundException | _: NoSuchFieldException => None
      }
    }.take(1).toSeq.headOption
  }

  private def invalidateWithRuntimeApi(workspaceId: String, artifactId: String): Boolean = {
    objectMethod(Seq(TokenLibraryClass), "invalidateMwcToken", 4).exists { case (module, method) =>
      method.invoke(
        module,
        workspaceId,
        artifactId,
        Int.box(SparkTokenVersion),
        SparkWorkloadType)
      true
    }
  }

  private def nfsCacheKey(cacheKey: String): String = {
    objectMethod(NfsCacheClasses, "getNFSCacheKey", 1)
      .map { case (module, method) => method.invoke(module, cacheKey).asInstanceOf[String] }
      .getOrElse(cacheKey)
  }

  private def deleteNfsToken(resolvedCacheKey: String): Boolean = {
    objectMethod(NfsCacheClasses, "getNFSTokenFilePath", 1).exists { case (module, method) =>
      val tokenPath = method.invoke(module, resolvedCacheKey) match {
        case path: Path => path
        case file: File => file.toPath
        case path: String => Paths.get(path)
        case other =>
          throw new IllegalStateException(
            s"Unsupported Fabric token cache path type: ${Option(other).map(_.getClass.getName).orNull}")
      }
      Files.deleteIfExists(tokenPath)
      true
    }
  }

  private def clearInMemoryTokenCache(): Boolean = {
    objectMethod(InMemoryCacheClasses, "clear", 0).exists { case (module, method) =>
      method.invoke(module)
      true
    }
  }

  private[ml] def invalidateSparkMwcTokenCaches(
      cacheKey: String,
      encodeNfsCacheKey: String => String,
      deleteNfsCacheEntry: String => Boolean,
      clearInMemoryCache: () => Boolean): Unit = {
    val deletedNfsToken = deleteNfsCacheEntry(encodeNfsCacheKey(cacheKey))
    val clearedInMemoryToken = clearInMemoryCache()
    if (!deletedNfsToken && !clearedInMemoryToken) {
      throw new NoSuchMethodException("Fabric runtime does not expose MWC token cache invalidation.")
    }
  }

  def invalidateSparkMwcToken(workspaceId: String, artifactId: String): Unit = {
    if (!invalidateWithRuntimeApi(workspaceId, artifactId)) {
      val cacheKey = workspaceId + artifactId + SparkTokenVersion + SparkWorkloadType
      invalidateSparkMwcTokenCaches(
        cacheKey,
        nfsCacheKey,
        deleteNfsToken,
        () => clearInMemoryTokenCache())
    }
  }

  def getMLWorkloadAADAuthHeader: String = "Bearer " + getAccessToken

  def getCognitiveMwcTokenAuthHeader(workspaceId: String, artifactId: String): String = "MwcToken " +
    getSparkMwcToken(workspaceId, artifactId)
}
