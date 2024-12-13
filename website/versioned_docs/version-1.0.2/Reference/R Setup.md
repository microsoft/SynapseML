---
title: R setup
hide_title: true
sidebar_label: R setup
description: R setup and example for SynapseML
---


# R setup and example for SynapseML

## Installation

**Requirements**: Ensure that R and
[devtools](https://github.com/hadley/devtools) installed on your
machine.

Also make sure you have Apache Spark installed. If you are using Sparklyr, you can use [spark-install](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_install.html). Be sure to specify the correct version. As of this writing, that should be version="3.2". spark_install is a bit eccentric and may install a slightly different version. Be sure that the version you get is one that you want.

On Windows, download [WinUtils.exe](https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe) and copy it into the `bin` directory of your Spark installation, e.g. C:\Users\user\AppData\Local\Spark\spark-3.3.2-bin-hadoop3\bin

To install the current SynapseML package for R, first install synapseml-core:

```R
...
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-core-0.11.0.zip")
...
```

and then install any or all of the following packages, depending on your intended usage:

synapseml-cognitive,
synapseml-deep-learning,
synapseml-lightgbm,
synapseml-opencv,
synapseml-vw

In other words:

```R
...
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-cognitive-0.11.0.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-deep-learning-0.11.0.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-lightgbm-0.11.0.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-opencv-0.11.0.zip")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-vw-0.11.0.zip")
...
```

### Importing libraries and setting up spark context

Installing all dependencies may be time-consuming.  When complete, run:

```R
...
library(sparklyr)
library(dplyr)
config <- spark_config()
config$sparklyr.defaultPackages <- "com.microsoft.azure:synapseml_2.12:1.0.2"
sc <- spark_connect(master = "local", config = config)
...
```

This creates a spark context on your local machine.

We then need to import the R wrappers:

```R
...
 library(synapseml.core)
 library(synapseml.cognitive)
 library(synapseml.deep.learning)
 library(synapseml.lightgbm)
 library(synapseml.opencv)
 library(synapseml.vw)
...
```

## Example

We can use the faithful dataset in R:

```R
...
faithful_df <- copy_to(sc, faithful)
cmd_model = ml_clean_missing_data(
              x=faithful_df,
              inputCols = c("eruptions", "waiting"),
              outputCols = c("eruptions_output", "waiting_output"),
              only.model=TRUE)
sdf_transform(cmd_model, faithful_df)
...
```

You should see the output:

```R
...
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
...
```

## Azure Databricks

In Azure Databricks, you can install devtools and the spark package from URL
and then use spark_connect with method = "databricks":

```R
install.packages("devtools")
devtools::install_url("https://mmlspark.blob.core.windows.net/rrr/synapseml-1.0.2.zip")
library(sparklyr)
library(dplyr)
sc <- spark_connect(method = "databricks")
faithful_df <- copy_to(sc, faithful)
unfit_model = ml_light_gbmregressor(sc, maxDepth=20, featuresCol="waiting", labelCol="eruptions", numIterations=10, unfit.model=TRUE)
ml_train_regressor(faithful_df, labelCol="eruptions", unfit_model)
```

## Building from Source

Our R bindings are built as part of the [normal build
process](../Developer%20Setup).  To get a quick build, start at the root
of the synapseml directory, and find the generated files. For instance,
to find the R files for deep-learning, run

```bash
sbt packageR
ls ./deep-learning/target/scala-2.12/generated/src/R/synapseml/R
```

You can then run R in a terminal and install the above files directly:

```R
...
devtools::install_local("./deep-learning/target/scala-2.12/generated/src/R/synapseml/R")
...
```
