if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

tar_path <- paste(getwd(), "/../../../../../../spark-4.1.1-bin-hadoop3.tgz", sep = "")

# If SPARK_HOME is already set (by CI pipeline), use it directly.
# Otherwise, install the tarball via sparklyr and try to locate it.
if (!nzchar(Sys.getenv("SPARK_HOME", ""))) {
  Sys.unsetenv("SPARK_HOME")
  if (file.exists(tar_path)) {
    spark_install_tar(tar_path)
  } else {
    fallback_path <- "spark-4.1.1-bin-hadoop3.tgz"
    if (file.exists(fallback_path)) {
      spark_install_tar(fallback_path)
    } else {
      print("Warning: Spark 4.1.1 tarball not found. Skipping explicit installation.")
    }
  }
}

options("testthat.output_file" = "../../../../r-test-results.xml")
if (!require("testthat")) {
  packages.install("testthat")
  library("testthat")
}
devtools::test(reporter = testthat::JunitReporter$new())
