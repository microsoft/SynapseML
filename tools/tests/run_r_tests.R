if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

#spark_available_versions(show_hadoop=TRUE, show_minor=TRUE)

spark_install(version = "3.3.2", hadoop_version = "3", logging="INFO", verbose=T)
options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
