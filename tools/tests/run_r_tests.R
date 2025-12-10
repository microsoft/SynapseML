if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

tar_path <- paste(getwd(), "/../../../../../../spark-4.0.1-bin-hadoop3.tgz", sep = "")
Sys.unsetenv("SPARK_HOME")
Sys.setenv(JAVA_HOME = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home")

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
