// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

// Auto-generate sub-project definitions

val _ = {

  val topDir      = file(".")
  val topName     = "MMLSpark"
  val ignoredDirs = Array(topDir / "project")

  def userError(message: String): Nothing = {
    System.err.println(message)
    System.err.println("Aborting...")
    sys.exit(1)
  }

  case class Proj(val dir: File, val children: List[Proj]) {
    val name = if (dir == topDir) topName
               else Project.normalizeModuleID(dir.toString.substring(2))
    val dirs = Iterator.iterate(dir)(_.getParentFile).takeWhile(_ != null)
                 .map(_.getName).toList.reverse.drop(1)
    val props = {
      val i = scala.io.Source.fromFile(dir / "build.sbt")
      val rx = "//> *(\\w+) *: *(.*?) *".r
      (try i.getLines.toList finally i.close)
        .map(_ match { case rx(x,y) => (x,y); case _ => null; }).filter(_ != null)
        .foldLeft(Map[String,List[String]]()) { (m,kv) =>
        m + (kv._1 -> (m.getOrElse(kv._1,Nil) :+ kv._2)) }
    }
    lazy val deps = children ++ props.getOrElse("DependsOn", Nil).map(nameToProj)
    def flatList(): List[Proj] = this +: children.flatMap(_.flatList)
    override def toString() = name
  }

  def findProjects(dir: File): List[Proj] = {
    val (dirs, files) = dir.listFiles().sorted.toList.partition(_.isDirectory)
    val nested = dirs.flatMap(findProjects)
    if (ignoredDirs.contains(dir) || !files.exists(p => p.getName == "build.sbt")) nested
    else List(Proj(dir, nested))
  }

  def nameToProj(name: String): Proj =
    nameProjMap.getOrElse(Project.normalizeModuleID(name),
                          userError(s"Bad project name: $name..."))

  // Cheap topological sort for projects; note that the input is sorted
  // alphabetically, and it preserves this order when posible
  def depSort(projs: List[List[Proj]]): List[Proj] = {
    if (projs.isEmpty) Nil
    else projs.find(_.tail.isEmpty) match {
      case Some(x +: _) => x +: depSort(projs.map(_.filterNot(_==x)).filterNot(_.isEmpty))
      case _ => userError(s"Dependency cycle! {${projs.map(_.head).mkString(", ")}}")
    }
  }

  lazy val topProj     = findProjects(topDir)(0)
  lazy val nameProjMap = topProj.flatList.map(p => (p.name -> p)).toMap
  lazy val sortedProjs = depSort(topProj.flatList.map(p => p +: p.deps))

  def projToSbt(proj: Proj): String = {
    def showList(list: List[Proj], what: String, sfx: String) = {
      if (list.isEmpty) ""
      else s"""\n  .${what}(\n    ${list.map(p => s"`$p`$sfx").mkString(",\n    ")})"""
    }
    (s"""val `$proj` = (project in ${("topDir" +: proj.dirs.map("\""+_+"\""))
                                     .mkString(" / ")})"""
       + "\n  .configs(IntegrationTest)"
       + "\n  .settings(Extras.defaultSettings: _*)"
       + showList(proj.children, "aggregate", "")
       // for the root project, use "optional" -- I don't know what it should
       // do, but it's visible in the POM file, which allows us to filter our
       // dependencies out of it (in "build.scala").
       + showList(proj.deps, "dependsOn",
                  if (proj != topProj) " % \"compile->compile;test->test\""
                  else " % \"compile->compile;optional\""))}

  IO.write(topDir / "autogen.sbt",
    s"""// Automatically generated, DO NOT EDIT\n
       |val topDir = file(".")\n
       |${sortedProjs.map(projToSbt).mkString("\n\n")}
       |""".stripMargin)

  IO.write(topDir / "project" / "autogen.scala",
    s"""// Automatically generated, DO NOT EDIT\n
       |object SubProjects {
       |  val all = Seq(
       |    ${sortedProjs.filter(_.children.isEmpty)
                .map("\"" + _.name + "\"").mkString(",\n    ")})
       |}
       |""".stripMargin)

  IO.write(topDir / "project" / "project-roots.txt",
           sortedProjs
             .map(p => {
                    val d = p.dir
                    (if (d == topDir) d else d.toString.substring(2)) + "\n"})
             .mkString(""))

  IO.write(topDir / "project" / "dependencies.digraph",
    s"""// Automatically generated, DO NOT EDIT\n
       |digraph ${topName} {
       |  ${sortedProjs.flatMap(p => p.deps.map(d => s""""$p" -> "$d";"""))
                       .mkString("\n  ")}
       |}
       |""".stripMargin)

}
