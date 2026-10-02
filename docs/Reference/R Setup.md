---
title: R setup
hide_title: true
sidebar_label: R setup
description: R setup and example for SynapseML
r_installation: source
---


# R setup and example for SynapseML

## Installation

**Requirements**: Install R and
[devtools](https://github.com/hadley/devtools) on your machine.

Also install a version of Apache Spark that is compatible with this SynapseML
release. If you are using sparklyr, you can use
[`spark_install`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_install.html).
On Windows, download
[WinUtils.exe](https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe)
and copy it into the `bin` directory of your Spark installation, for example,
`C:\Users\user\AppData\Local\Spark\spark-3.5.0-bin-hadoop3\bin`.

Automated public releases publish Maven artifacts and Python wrappers. They do
not publish R ZIP archives. Build the R wrappers from the matching release tag;
do not construct an R download URL by substituting the new release version.

Select the tag for your runtime from the
[installation matrix](../Get%20Started/Install%20SynapseML.md), check out that
tag, and follow [Developer Setup](Developer%20Setup.md) for its toolchain.
From the repository root, build the local R packages:

```bash
sbt packageR
```

Then start R from the same repository root. Set `scala_binary` to `2.12` for a
Spark 3.5 checkout or `2.13` for a Spark 4 checkout. The following installs the
generated package directories, not unpublished downloads:

```R
scala_binary <- "2.12"
modules <- c("core", "cognitive", "opencv", "deep-learning", "lightgbm", "vw")
for (module in modules) {
  package_dir <- file.path(
    module, "target", paste0("scala-", scala_binary),
    "generated", "src", "R", "synapseml"
  )
  stopifnot(file.exists(file.path(package_dir, "DESCRIPTION")))
  devtools::install_local(package_dir, upgrade = "never")
}
```

Provide the released Maven coordinate and Blob resolver explicitly, and pass
`extensions = character()` as shown below. A local build's generated extension
metadata can name a snapshot coordinate; it must not override the chosen release.

### Importing libraries and setting up a Spark context

After installing the local wrappers, create the
Spark context with an explicit package coordinate and repository. For local
sparklyr connections, `sparklyr.shell.repositories` supplies the repository to
`spark-submit`, while `extensions = character()` prevents the wrappers' embedded
registration from overriding it:

> The examples below use Spark 3.5 / Scala 2.12 with
> `com.microsoft.azure:synapseml_2.12:1.1.3`. For Spark 4.0 use
> `com.microsoft.azure:synapseml_2.13:1.1.3-spark4.0`; for Spark 4.1 use
> `com.microsoft.azure:synapseml_2.13:1.1.3-spark4.1`.

```R
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.defaultPackages <- "com.microsoft.azure:synapseml_2.12:1.1.3"
config$sparklyr.shell.repositories <- "https://mmlspark.blob.core.windows.net/maven"
sc <- spark_connect(
  master = "local",
  config = config,
  extensions = character()
)
```

Then import the installed R wrappers:

```R
library(synapseml.core)
library(synapseml.cognitive)
library(synapseml.deep.learning)
library(synapseml.lightgbm)
library(synapseml.opencv)
library(synapseml.vw)
```

## Example

We can use the faithful dataset in R:

```R
faithful_df <- copy_to(sc, faithful)
cmd_model <- ml_clean_missing_data(
  x = faithful_df,
  inputCols = c("eruptions", "waiting"),
  outputCols = c("eruptions_output", "waiting_output"),
  only.model = TRUE
)
ml_transform(cmd_model, faithful_df)
```

You should see output similar to:

```text
# Source:   table<sparklyr_tmp_17d66a9d490c> [?? x 4]
# Database: spark_connection
   eruptions waiting eruptions_output waiting_output
       <dbl>   <dbl>            <dbl>          <dbl>
 1     3.600      79            3.600             79
 2     1.800      54            1.800             54
 3     3.333      74            3.333             74
 4     2.283      62            2.283             62
 5     4.533      85            4.533             85
 6     2.883      55            2.883             55
 7     4.700      88            4.700             88
 8     3.600      85            3.600             85
 9     1.950      51            1.950             51
10     4.350      85            4.350             85
# ... with more rows
```

## Azure Databricks

Install the locally built R packages from the installation block above on the
cluster driver. SynapseML's JVM package must be available when the cluster
starts; `spark_connect(method = "databricks")` connects to an existing Spark
session and cannot add the JVM package afterward. Before starting or restarting
the cluster, either:

- add the Maven library `com.microsoft.azure:synapseml_2.12:1.1.3` and set its
  repository (under **Advanced options**) to
  `https://mmlspark.blob.core.windows.net/maven`; or
- add both settings to the cluster's Spark configuration:

```text
spark.jars.packages com.microsoft.azure:synapseml_2.12:1.1.3
spark.jars.repositories https://mmlspark.blob.core.windows.net/maven
```

After the cluster restarts, connect without loading the embedded extension
metadata:

```R
library(sparklyr)
library(dplyr)
library(synapseml.core)
library(synapseml.lightgbm)

sc <- spark_connect(method = "databricks", extensions = character())
faithful_df <- copy_to(sc, faithful)
unfit_model <- ml_light_gbm_regressor(
  sc,
  maxDepth = 20,
  featuresCol = "waiting",
  labelCol = "eruptions",
  numIterations = 10,
  unfit.model = TRUE
)
ml_train_regressor(faithful_df, labelCol = "eruptions", model = unfit_model)
```

## Historical releases

[Older versioned guides](https://github.com/microsoft/SynapseML/tree/master/website/versioned_docs)
retain their original archive instructions. Those historical wrappers are not
automatically compatible with a newer Maven release.
