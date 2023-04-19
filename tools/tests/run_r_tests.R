if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

spark_install(version = "3.2.4", hadoop_version = "2.7", logging="INFO", verbose=T)
options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
