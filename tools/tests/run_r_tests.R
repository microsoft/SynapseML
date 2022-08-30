if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

spark_install(version = "3.2.2", hadoop_version = "3.2")
options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())