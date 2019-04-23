# R setup and example for MMLSpark

## Installation

**Requirements**: You will need to have R and
[devtools](https://github.com/hadley/devtools) installed on your
machine.

To install the current MMLSpark package for R use:

   ```R
   ...
   devtools::install_url("https://mmlspark.azureedge.net/rrr/mmlspark-0.17.zip")
   ...
   ```

### Importing libraries and setting up spark context

It will take some time to install all dependencies.  Then, run:

   ```R
   ...
   library(sparklyr)
   library(dplyr)
   config <- spark_config()
   config$sparklyr.defaultPackages <- "Azure:mmlspark:0.17"
   sc <- spark_connect(master = "local", config = config)
   ...
   ```

This will create a spark context on local machine.

We will then need to import the R wrappers:

   ```R
   ...
   library(mmlspark)
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
   devtools::install_url("https://mmlspark.azureedge.net/rrr/mmlspark-0.17.zip")
   library(sparklyr)
   library(dplyr)
   sc <- spark_connect(method = "databricks")
   faithful_df <- copy_to(sc, faithful)
   unfit_model = ml_light_gbmregressor(sc, maxDepth=20, featuresCol="waiting", labelCol="eruptions", numIterations=10, unfit.model=TRUE)
   ml_train_regressor(faithful_df, labelCol="eruptions", unfit_model)
   ```

## Building from Source

Our R bindings are built as part of the [normal build
process](developer-readme.md).  To get a quick build, start at the root
of the mmlspark directory, and:

   ```bash
   ./runme TESTS=NONE
   unzip ./BuildArtifacts/packages/R/mmlspark-0.0.zip
   ```

You can then run R in a terminal and install the above files directly:

   ```R
   ...
   devtools::install_local("./BuildArtifacts/packages/R/mmlspark")
   ...
   ```
