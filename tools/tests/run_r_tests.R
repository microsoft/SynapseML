if (!require("sparklyr")) {
  packages.install("sparklyr")
  library("sparklyr")
}

if (!nzchar(Sys.getenv("SPARK_HOME", ""))) {
  spark_archive <- paste(getwd(), "/../../../../../../spark-3.5.0-bin-hadoop3.tgz", sep = "")
  spark_install_tar(spark_archive)
  installed_spark_home <- file.path(
    spark_install_dir(),
    tools::file_path_sans_ext(basename(spark_archive))
  )
  if (!dir.exists(installed_spark_home)) {
    stop("Unable to locate Spark after installing ", spark_archive)
  }
  Sys.setenv(SPARK_HOME = installed_spark_home)
}

options("testthat.output_file" = "../../../../r-test-results.xml")
devtools::test(reporter = JunitReporter$new())
