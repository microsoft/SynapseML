package com.microsoft.ml.spark.tvm

import org.apache.tvm.{Function, TVMValue}

class TVMModel {
  def funcDefExample(): Unit ={
    val func: Function = Function.convertFunc(
      new Function.Callback {
        override def invoke(args: TVMValue*): String = {
          val res: StringBuilder = new StringBuilder
          res.toString
        }
      }
    )
    val res: TVMValue = func.pushArg("Hello").pushArg(" ").pushArg("World!").invoke
    res.release()
    func.release()
  }

  def loadModel(args: Array[String]): Unit = {

  }
}
