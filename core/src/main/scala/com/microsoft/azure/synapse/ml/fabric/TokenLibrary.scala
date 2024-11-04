// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

object TokenLibrary {
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
    methodMirror(workspaceId, artifactId, 2, "SparkCore")
      .asInstanceOf[String]
  }


  def getMLWorkloadAADAuthHeader: String = "Bearer " + getAccessToken

  def getCognitiveMwcTokenAuthHeader(workspaceId: String, artifactId: String): String = "MwcToken " +
    getSparkMwcToken(workspaceId, artifactId)
}
