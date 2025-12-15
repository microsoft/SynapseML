if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

tar_path <- paste(getwd(), "/../../../../../../spark-4.0.1-bin-hadoop3.tgz", sep = "")
Sys.unsetenv("SPARK_HOME")

# Prefer an existing JAVA_HOME if one is set; otherwise, try to infer it from
# the java binary on PATH. This keeps the script portable across Linux/macOS
# and avoids hard-coding a local JDK path.
java_home <- Sys.getenv("JAVA_HOME", unset = NA)
if (is.na(java_home) || !nzchar(java_home) || !dir.exists(java_home)) {
  java_bin <- Sys.which("java")
  if (nzchar(java_bin)) {
    candidate <- dirname(dirname(java_bin))
    if (dir.exists(candidate)) {
      java_home <- candidate
    }
  }
}

if (!is.na(java_home) && nzchar(java_home) && dir.exists(java_home)) {
  Sys.setenv(JAVA_HOME = java_home)
} else {
  message("Warning: JAVA_HOME is not set and could not be inferred from PATH; sparklyr may fail to locate Java.")
}

if (file.exists(tar_path)) {
  spark_install_tar(tar_path)
} else {
  print(paste("Spark tarball not found at", tar_path))
  # Try to find it in the project root if we are running from there or close to it
  # This is a fallback for local dev or if the path depth is different
  fallback_path <- "spark-4.0.1-bin-hadoop3.tgz"
  if (file.exists(fallback_path)) {
    spark_install_tar(fallback_path)
  } else {
    print("Warning: Spark 4.0.1 tarball not found. Skipping explicit installation.")
  }
}

options("testthat.output_file" = "../../../../r-test-results.xml")
if (!require("testthat")) {
  packages.install("testthat")
  library("testthat")
}
devtools::test(reporter = testthat::JunitReporter$new())
