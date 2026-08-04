---
title: R setup
hide_title: true
sidebar_label: R setup
description: R setup and example for SynapseML
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
`C:\Users\user\AppData\Local\Spark\spark-3.3.2-bin-hadoop3\bin`.

The R bindings are published as one archive per SynapseML module. A combined
`synapseml-1.1.2.zip` archive is not published. Install `synapseml-core` and
the modules needed by your application (the following installs all six):

```R
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-core-1.1.2.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-cognitive-1.1.2.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-deep-learning-1.1.2.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-lightgbm-1.1.2.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-opencv-1.1.2.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-vw-1.1.2.zip")
```

> **Published archive compatibility:** The component archives for this release
> were generated before the artifact endpoint migration and embed the retired
> Azure CDN Maven resolver in their sparklyr extension registration. Until the
> archives are regenerated and published, provide the Blob resolver explicitly
> and pass `extensions = character()` as shown below. Otherwise, loading an R
> wrapper before connecting can reactivate the retired resolver.

### Importing libraries and setting up a Spark context

Installing all dependencies may be time-consuming. When complete, create the
Spark context with an explicit package coordinate and repository. For local
sparklyr connections, `sparklyr.shell.repositories` supplies the repository to
`spark-submit`, while `extensions = character()` prevents the wrappers' embedded
registration from overriding it:

```R
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.defaultPackages <- "com.microsoft.azure:synapseml_2.12:1.1.2"
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

Install the R component archives from the installation block above on the
cluster driver. SynapseML's JVM package must be available when the cluster
starts; `spark_connect(method = "databricks")` connects to an existing Spark
session and cannot add the JVM package afterward. Before starting or restarting
the cluster, either:

- add the Maven library `com.microsoft.azure:synapseml_2.12:1.1.2` and set its
  repository (under **Advanced options**) to
  `https://mmlspark.blob.core.windows.net/maven`; or
- add both settings to the cluster's Spark configuration:

```text
spark.jars.packages com.microsoft.azure:synapseml_2.12:1.1.2
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

## Building from Source

Our R bindings are built as part of the [normal build
process](../Developer%20Setup). To get a quick build, start at the root
of the SynapseML directory and find the generated files. For example, to find
the R files for deep-learning, run:

```bash
sbt packageR
ls ./deep-learning/target/scala-2.12/generated/src/R/synapseml/R
```

You can then run R in a terminal and install the files directly:

```R
devtools::install_local("./deep-learning/target/scala-2.12/generated/src/R/synapseml/R")
```
