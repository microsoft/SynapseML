import sbt._

/**
 * Centralized dependency and version management for SynapseML.
 *
 * This file consolidates all version definitions and dependency declarations
 * to ensure consistency across modules and make upgrades easier.
 */
object Versions {
  // Core framework versions
  val spark = "3.5.0"
  val scala = "2.12.17"
  val scalaMajor = "2.12"

  // Testing
  val scalatest = "3.2.14"
  val scalactic = "3.2.14"

  // HTTP clients
  val httpclient5 = "5.1.3"
  val httpmime = "4.5.13"

  // Utilities
  val commonsLang = "2.6"
  val sprayJson = "1.3.5"
  val jsch = "0.1.54"

  // ML libraries
  val isolationForest = "3.0.5"

  // Test utilities
  val hadoopBareNakedLocalFs = "0.1.0"
}

/**
 * Exclusion rules for managing transitive dependency conflicts.
 * Each exclusion is documented with the reason it exists.
 */
object Exclusions {
  import Versions._

  /**
   * spark-tags is a test-only artifact from Spark. Excluding prevents
   * it from leaking into production JARs via transitive dependencies.
   */
  val sparkTags = ExclusionRule("org.apache.spark", s"spark-tags_$scalaMajor")

  /**
   * We standardize on scalatest 3.2.14. Excluding prevents older
   * versions from entering via transitive Spark test dependencies.
   */
  val scalatest = ExclusionRule("org.scalatest")

  /**
   * Breeze is a numerical processing library. Spark bundles its own
   * version, so we exclude to avoid conflicts.
   */
  val breeze = ExclusionRule("org.scalanlp", s"breeze_$scalaMajor")

  /**
   * Protobuf exclusion for isolation-forest. Spark bundles protobuf 3.x
   * which can conflict with other libraries expecting different versions.
   */
  val protobuf = ExclusionRule("com.google.protobuf", "protobuf-java")

  /**
   * Standard set of exclusions applied to most dependencies
   * to prevent common transitive dependency conflicts.
   */
  val standard: Seq[ExclusionRule] = Seq(sparkTags, scalatest, breeze)
}
