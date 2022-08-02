library("sparklyr")
spark_install(version = "3.2.2", hadoop_version = "3.2")
options("testthat.output_file" = "../../../../r-test-results.xml")
tryCatch({
  print(paste("SPARK_HOME=", Sys.getenv("SPARK_HOME")))
  devtools::test(reporter = JunitReporter$new())
},
error = function(err) {
  print(paste("Error: ", err))
  stop(err)
})