// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import sbt._
import scala.io._

// Warn user when the library configuration has changed
object LibraryCheck {
  def apply() = ()
  val info = file(sys.env.getOrElse("HOME", System.getProperty("user.home"))) /
             ".mmlspark_installed_libs"
  val conf = file("..") / "tools" / "config.sh"
  val (len, modif) = (conf.length, conf.lastModified)
  def read[T](file: File, read: BufferedSource => T): T = {
    val i = Source.fromFile(file); try read(i) finally i.close
  }
  lazy val text =
    "(?s)INSTALLATIONS=\\(.*?\r?\n\\)\r?\n".r.findFirstIn(read(conf, _.mkString)).get
  lazy val (len_, modif_, text_) =
    read(info, i => {
           val meta = i.getLines.take(2).toList.map(_.toLong)
           (meta(0), meta(1), i.mkString) })
  def writeInfo() = scala.tools.nsc.io.File(info).writeAll(s"$len\n$modif\n$text")
  if (!info.exists) writeInfo()
  else if (len_ != len || modif_ != modif) {
    if (text_ == text) writeInfo()
    else {
      println("\n!!! Warning: Library configuration changed,"
              + " consider using ./runme to update !!!\n")
      Thread.sleep(1000)
    }
  }
}
