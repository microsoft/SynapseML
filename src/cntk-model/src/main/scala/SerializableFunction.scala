// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.CNTK

import java.io._
import java.util.UUID._

import com.microsoft.CNTK.CNTKUtils._
import com.microsoft.ml.spark.StreamUtilities.using
import org.apache.commons.io.FileUtils.{forceDelete, getTempDirectoryPath, writeByteArrayToFile}
import org.apache.commons.io.IOUtils
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, StructField, DataType => SDataType}

import scala.language.implicitConversions

object CNTKExtensions {
  implicit def toSerializable(in: Function): SerializableFunction = new SerializableFunction(in)

  implicit def fromSerializable(in: SerializableFunction): Function = in.fvar
}

object CNTKUtils {

  type GenericVectorVector = Either[FloatVectorVector, DoubleVectorVector]

  def deleteGVV(fvv: GenericVectorVector): Unit = {
    fvv match {
      case Left(vv) =>
        val size = vv.size().toInt
        (0 until size).foreach{i =>
          vv.get(i).delete()
        }
        vv.delete()
      case Right(vv) =>
        val size = vv.size().toInt
        (0 until size).foreach{i =>
          vv.get(i).delete()
        }
        vv.delete()
    }
  }

  def toSeqSeq(vv: FloatVectorVector): Seq[Seq[Float]] = {
    (0 until vv.size.toInt).map { i =>
      val v = vv.get(i)
      (0 until v.size.toInt).map { j =>
        v.get(j)
      }
    }
  }

  def toSeqDV(vv: FloatVectorVector): Seq[DenseVector] = {
    (0 until vv.size.toInt).map { i =>
      val v = vv.get(i)
      new DenseVector((0 until v.size.toInt).map { j =>
        v.get(j).toDouble
      }.toArray)
    }
  }

  def toSeqSeq(vv: DoubleVectorVector): Seq[Seq[Double]] = {
    (0 until vv.size.toInt).map { i =>
      val v = vv.get(i)
      (0 until v.size.toInt).map { j =>
        v.get(j)
      }
    }
  }

  def toSeqDV(vv: DoubleVectorVector): Seq[DenseVector] = {
    (0 until vv.size.toInt).map { i =>
      val v = vv.get(i)
      new DenseVector((0 until v.size.toInt).map { j =>
        v.get(j)
      }.toArray)
    }
  }

  def SSFToGVV(arr: Seq[Seq[Float]]): GenericVectorVector = {
    val inputFVV = new FloatVectorVector(arr.length.toLong)
    arr.zipWithIndex.foreach { case (vect, i) =>
      val fv = new FloatVector(vect.length.toLong)
      vect.zipWithIndex.foreach { case (x, j) =>
        fv.set(j, x)
      }
      inputFVV.set(i, fv)
    }
    Left(inputFVV)
  }

  def SSDToGVV(arr: Seq[Seq[Double]]): GenericVectorVector = {
    val inputDVV = new DoubleVectorVector(arr.length.toLong)
    arr.zipWithIndex.foreach { case (vect, i) =>
      val dv = new DoubleVector(vect.length.toLong)
      vect.zipWithIndex.foreach { case (x, j) =>
        dv.set(j, x)
      }
      inputDVV.set(i, dv)
    }
    Right(inputDVV)
  }

  def loadModelFromBytes(bytes: Array[Byte],
                         device: DeviceDescriptor =
                         DeviceDescriptor.useDefaultDevice): Function = {
    try {
      Function.load(bytes, device)
    } catch {
      case _: IllegalArgumentException =>
        // println("WARNING: model is a legacy model," +
        //           " consider using a more recent model for faster loading")
        import java.util.UUID._
        val modelFile = new File(s"$getTempDirectoryPath/$randomUUID.model")
        writeByteArrayToFile(modelFile, bytes)
        val model = try {
          Function.load(modelFile.getPath, device)
        } finally forceDelete(modelFile)
        model
    }
  }

  def variableToElementType(v: Variable): SDataType = {
    v.getDataType match {
      case DataType.Double => DoubleType
      case DataType.Float => FloatType
      case t => throw new IllegalArgumentException(s"Data Type $t not supported")
    }
  }

  def variableToSchema(v: Variable): SDataType = {
    val baseType: SDataType = variableToElementType(v)
    (1 to v.getShape.getRank.toInt).foldLeft(baseType) {case (t,_) => ArrayType(t)}
  }

  def variableToArraySchema(v: Variable): SDataType = {
    ArrayType(variableToElementType(v))
  }

  val argumentPrefix = "argument_" // used for indexing into the arguments array of the model

  val outputPrefix = "output_" // used for indexing into the outputs array of the modes

}

object SerializableFunction {

  import CNTKExtensions._

  def load(filepath: String): SerializableFunction = Function.load(filepath)

  def load(buffer: Array[Byte], device: DeviceDescriptor): SerializableFunction =
    Function.load(buffer, device)

  def load(filepath: String, device: DeviceDescriptor): SerializableFunction =
    Function.load(filepath, device)

  def loadModelFromBytes(bytes: Array[Byte],
                         device: DeviceDescriptor = DeviceDescriptor.useDefaultDevice()): SerializableFunction =
    CNTKUtils.loadModelFromBytes(bytes, device)

}

class SerializableFunction(f: Function) extends Serializable {
  var fvar: Function = f

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    val modelFile = new File(s"$getTempDirectoryPath/$randomUUID.model")
    fvar.save(modelFile.toString)
    val bytes = try {
      using(new FileInputStream(modelFile)){ fis =>
        IOUtils.toByteArray(fis)
      }.get
    } finally {
      forceDelete(modelFile)
    }
    out.writeObject(bytes)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    val bytes = in.readObject().asInstanceOf[Array[Byte]]
    val device = DeviceDescriptor.useDefaultDevice()
    fvar = CNTKUtils.loadModelFromBytes(bytes, device)
  }

  private def findByNameSafe(name: String): Variable = {
    Option(fvar.findByName(name)).map(_.getOutput).getOrElse(
      throw new IllegalArgumentException(s"Node $name could not be found in the model")
    )
  }

  def getInputVar(name: String): Variable = {
    if (name.startsWith(CNTKUtils.argumentPrefix)){
      fvar.getArguments.get(name.stripPrefix(CNTKUtils.argumentPrefix).toInt)
    }else{
      findByNameSafe(name)
    }
  }

  def getOutputVar(name: String): Variable = {
    if (name.startsWith(CNTKUtils.outputPrefix)){
      fvar.getOutputs.get(name.stripPrefix(CNTKUtils.outputPrefix).toInt)
    }else{
      findByNameSafe(name)
    }
  }

  def getInputTypes(feedDict: Map[String, String]): List[StructField] = {
    feedDict.toList.sorted.map { case (varname, colname) =>
      StructField(colname, variableToArraySchema(getInputVar(varname)))
    }
  }

  def getOutputSchema(fetchDict: Map[String, String]): List[StructField] = {
    fetchDict.toList.sorted.map { case (colname, varname) =>
      StructField(colname, variableToArraySchema(getOutputVar(varname)))
    }
  }

}
