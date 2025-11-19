if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

spark_install_tar(paste(getwd(), "/../../../../../../spark-4.0.1-bin-hadoop3.tgz", sep = ""))

options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
