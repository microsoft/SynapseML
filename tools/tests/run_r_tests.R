library("sparklyr")
print(spark_available_versions(show_hadoop=TRUE, show_minor=TRUE))
#spark_install(version = "3.2.2", hadoop_version = "3.2")
tryCatch({
  spark_install_find(version = "3.2.1", hadoop_version = "3")
},
error=function(err) {
  print("Installing ../../../../../../../spark-3.2.1-bin-hadoop3.2.tgz")
  spark_install_tar("../../../../../../../spark-3.2.1-bin-hadoop3.2.tgz")
})
options("testthat.output_file" = "../../../../r-test-results.xml")
tryCatch({
  print(paste("SPARK_HOME=", Sys.getenv("SPARK_HOME")))
  devtools::test(reporter = JunitReporter$new())
},
error = function(err) {
  print(paste("Error: ", err))
  stop(err)
})