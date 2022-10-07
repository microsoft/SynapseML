if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

spark_install(version = "3.3.0", hadoop_version = "3")
# https://archive.apache.org/dist/spark/spark-3.3.0/
options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
