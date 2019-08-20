// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cntk

import com.microsoft.CNTK.{DoubleVector, DoubleVectorVector, FloatVector, FloatVectorVector}
import org.apache.spark.ml.linalg.{Vector=>SVector, Vectors}

import scala.language.implicitConversions

object ConversionUtils {

  type GVV = Either[FloatVectorVector, DoubleVectorVector]

  type SSG = Either[Seq[Seq[Float]], Seq[Seq[Double]]]

  def convertGVV(gvv: GVV): Seq[Seq[_]] = {
    val ssg =toSSG(gvv)
    ssg.left.toOption.getOrElse(ssg.right.get)
  }

  def toSSG(gvv: GVV): SSG = {
    gvv match {
      case Left(vv) =>
        Left((0 until vv.size.toInt).map { i =>
          val v = vv.get(i)
          (0 until v.size.toInt).map { j =>
            v.get(j)
          }
        })
      case Right(vv) =>
        Right((0 until vv.size.toInt).map { i =>
          val v = vv.get(i)
          (0 until v.size.toInt).map { j =>
            v.get(j)
          }
        })
    }
  }

  def deleteGVV(gvv: GVV): Unit = {
    gvv match {
      case Left(fvv) => fvv.clear(); fvv.delete()
      case Right(dvv) => dvv.clear(); dvv.delete()
    }
  }

  def toDV(gvv: GVV): Seq[SVector] = {
    gvv match {
      case Left(vv) =>
        (0 until vv.size.toInt).map { i =>
          val v = vv.get(i)
          Vectors.dense((0 until v.size.toInt).map { j =>
            v.get(j).toDouble
          }.toArray)
        }
      case Right(vv) =>
        (0 until vv.size.toInt).map { i =>
          val v = vv.get(i)
          Vectors.dense((0 until v.size.toInt).map { j =>
            v.get(j)
          }.toArray)
        }
    }

  }

  def toFV(v: Seq[Float], fv: FloatVector): FloatVector = {
    val vs = v.size
    val fvs = fv.size()
    if (fvs==vs) {
      ()
      v.zipWithIndex.foreach(p => fv.set(p._2, p._1))
    } else if (fvs>vs) {
      fv.clear()
      fv.reserve(vs.toLong)
      v.foreach(fv.add)
    } else {
      fv.reserve(vs.toLong)
      (0 until fvs.toInt).foreach(i => fv.set(i, v(i)))
      (fvs.toInt until vs).foreach(i => fv.add(v(i)))
    }
    fv
  }

  def toDV(v: Seq[Double], fv: DoubleVector): DoubleVector = {
    val vs = v.size
    val fvs = fv.size()
    if (fvs==vs) {
      ()
      v.zipWithIndex.foreach(p => fv.set(p._2, p._1))
    } else if (fvs>vs) {
      fv.clear()
      fv.reserve(vs.toLong)
      v.foreach(fv.add)
    } else {
      fv.reserve(vs.toLong)
      (0 until fvs.toInt).foreach(i => fv.set(i, v(i)))
      (fvs.toInt until vs).foreach(i => fv.add(v(i)))
    }
    fv
  }

  def toFV(v: Seq[Float]): FloatVector = {
    val fv = new FloatVector(v.length.toLong)
    v.zipWithIndex.foreach(p=>fv.set(p._2,p._1))
    fv
  }

  def toDV(v: Seq[Double]): DoubleVector = {
    val fv = new DoubleVector(v.length.toLong)
    v.zipWithIndex.foreach(p=>fv.set(p._2,p._1))
    fv
  }

  def toFVV(vv: Seq[Seq[Float]], fvv: FloatVectorVector): FloatVectorVector = {
    val vvs = vv.size
    val fvvs = fvv.size()
    if (fvvs==vvs) {
      ()
      vv.zipWithIndex.foreach(p=>toFV(p._1,fvv.get(p._2)))
    } else if (fvvs>vvs) {
      fvv.clear()
      fvv.reserve(vvs.toLong)
      vv.foreach { v => fvv.add(toFV(v))}
    } else {
      fvv.reserve(vvs.toLong)
      (0 until fvvs.toInt).foreach(i => fvv.set(i, toFV(vv(i),fvv.get(i))))
      (fvvs.toInt until vvs).foreach(i => fvv.add(toFV(vv(i))))
    }
    fvv
  }

  def toDVV(vv: Seq[Seq[Double]], fvv: DoubleVectorVector): DoubleVectorVector = {
    val vvs = vv.size
    val fvvs = fvv.size()
    if (fvvs==vvs) {
      ()
      vv.zipWithIndex.foreach(p=>toDV(p._1,fvv.get(p._2)))
    } else if (fvvs>vvs) {
      fvv.clear()
      fvv.reserve(vvs.toLong)
      vv.foreach { v => fvv.add(toDV(v))}
    } else {
      fvv.reserve(vvs.toLong)
      (0 until fvvs.toInt).foreach(i => fvv.set(i, toDV(vv(i),fvv.get(i))))
      (fvvs.toInt until vvs).foreach(i => fvv.add(toDV(vv(i))))
    }
    fvv
  }

  def toGVV(garr: SSG, existingGVV: GVV): GVV = {
    (garr, existingGVV) match {
      case (Left(arr), Left(fvv)) =>
        Left(toFVV(arr,fvv))
      case (Right(arr), Right(fvv)) =>
        Right(toDVV(arr,fvv))
      case _ =>
        throw new IllegalArgumentException("Need to have matching arrays and VectorVectors")
    }
  }

}
