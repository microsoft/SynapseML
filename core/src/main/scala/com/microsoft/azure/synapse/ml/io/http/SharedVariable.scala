// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

/** Holds a variable shared among all workers. Useful to use non-serializable objects in Spark closures.
  *
  * Note this code has been borrowed from:
  * https://www.nicolaferraro.me/2016/02/22/using-non-serializable-objects-in-apache-spark/
  *
  * @author Nicola Ferraro
  */
class SharedVariable[T: ClassTag](constructor: => T) extends AnyRef with Serializable {

  @transient private lazy val instance: T = constructor

  def get: T = instance

}

object SharedVariable {

  def apply[T: ClassTag](constructor: => T): SharedVariable[T] = new SharedVariable[T](constructor)

}

/** Holds a variable shared among all workers that behaves like a local singleton.
  * Useful to use non-serializable objects in Spark closures that maintain state across tasks.
  *
  * @author Nicola Ferraro
  */
class SharedSingleton[T: ClassTag](constructor: => T) extends AnyRef with Serializable {

  val singletonUUID: String = UUID.randomUUID().toString

  @transient private lazy val instance: T = {
    SharedSingleton.SingletonPool.synchronized {
      val singletonOption = SharedSingleton.SingletonPool.get(singletonUUID)
      if (singletonOption.isEmpty) {
        SharedSingleton.SingletonPool.put(singletonUUID, constructor)
      }
    }
    SharedSingleton.SingletonPool(singletonUUID).asInstanceOf[T]
  }

  def get: T = instance
}

object SharedSingleton {

  private val SingletonPool = new TrieMap[String, Any]()

  def apply[T: ClassTag](constructor: => T): SharedSingleton[T] = new SharedSingleton[T](constructor)

  def poolSize: Int = SingletonPool.size

  def poolClear(): Unit = SingletonPool.clear()

}
