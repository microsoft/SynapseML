if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

spark_install(version = "3.3.1", hadoop_version = "3")
options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
