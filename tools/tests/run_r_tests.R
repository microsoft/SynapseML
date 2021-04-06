library("sparklyr")
tryCatch({
  spark_install_find(version = "3.0.1")
},
  error=function(err) {
    spark_install_tar("../../../../../../spark-3.0.1-bin-hadoop3.2.tgz")
  }
)

options("testthat.output_file" = "../../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())