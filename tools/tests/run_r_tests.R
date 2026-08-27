if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

if (!nzchar(Sys.getenv("SPARK_HOME", ""))) {
  spark_install_tar(paste(getwd(), "/../../../../../../spark-3.5.0-bin-hadoop3.tgz", sep = ""))
}

options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
