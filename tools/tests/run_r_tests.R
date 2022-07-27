library("sparklyr")
spark_install(version = "3.2.1", hadoop_version = "3.2")
options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
