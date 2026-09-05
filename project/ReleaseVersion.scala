// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import java.io.File
import scala.sys.process.Process

object ReleaseVersion {
  def resolve(dynverVersion: String,
              environment: Map[String, String],
              directory: File = new File(".")): String = {
    environment.get("SYNAPSEML_RELEASE_TAG").filter(_.nonEmpty) match {
      case None =>
        if (dynverVersion.endsWith("-SNAPSHOT")) dynverVersion else s"$dynverVersion-SNAPSHOT"
      case Some(ref) =>
        val tag = ref.stripPrefix("refs/tags/")
        val versionPattern = "v(?:0|[1-9][0-9]*)\\.(?:0|[1-9][0-9]*)\\.(?:0|[1-9][0-9]*)" +
          "(?:-(?:spark|python)[0-9]+\\.[0-9]+)?"
        require(tag.matches(versionPattern), "Explicit release tag is not a supported version")
        val commit = environment.getOrElse("SYNAPSEML_RELEASE_COMMIT", "")
        val planId = environment.getOrElse("SYNAPSEML_RELEASE_PLAN_ID", "")
        require(commit.matches("[0-9a-f]{40}"), "Explicit release needs a reviewed commit")
        require(planId.matches("[0-9a-f]{64}"), "Explicit release needs an approved plan ID")

        def git(arguments: String*): String = {
          Process(Seq("git") ++ arguments, directory).!!.trim
        }

        require(git("rev-parse", "HEAD") == commit, "Release HEAD differs from the approved commit")
        require(git("rev-parse", s"refs/tags/$tag^{commit}") == commit,
          "Release tag differs from the approved commit")
        require(git("status", "--porcelain", "--untracked-files=normal").isEmpty,
          "A dirty checkout cannot produce a release version")
        tag.stripPrefix("v")
    }
  }

  def mayOverwrite(container: String, destination: String): Boolean = {
    val path = destination.toLowerCase(java.util.Locale.ROOT)
    container match {
      case "maven" => path.stripSuffix("/").endsWith("-snapshot")
      case "pip" => path.takeWhile(_ != '/').endsWith("-snapshot")
      case "rrr" => path.stripSuffix(".zip").endsWith("-snapshot")
      case _ => true
    }
  }
}
