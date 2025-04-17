---
title: Quickstart - LightGBM in Dotnet
sidebar_label: Quickstart - LightGBM in Dotnet
description: A simple example about classification with LightGBMClassifier using .NET
---

:::note
Make sure you have followed the guidance in [.NET installation](../Dotnet%20Setup) before jumping into this example.
:::

## Classification with LightGBMClassifier

Install NuGet packages by running following command:
```powershell
dotnet add package Microsoft.Spark --version 2.1.1
dotnet add package SynapseML.Lightgbm --version 1.0.11
dotnet add package SynapseML.Core --version 1.0.11
```

Use the following code in your main program file:
```csharp
using System;
using System.Collections.Generic;
using Synapse.ML.Lightgbm;
using Synapse.ML.Featurize;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace SynapseMLApp
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create Spark session
            SparkSession spark =
                SparkSession
                    .Builder()
                    .AppName("LightGBMExample")
                    .GetOrCreate();

            // Load Data
            DataFrame df = spark.Read()
                .Option("inferSchema", true)
                .Parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet")
                .Limit(2000);

            var featureColumns = new string[] {"age", "workclass", "fnlwgt", "education", "education-num",
                "marital-status", "occupation", "relationship", "race", "sex", "capital-gain",
                "capital-loss", "hours-per-week", "native-country"};

            // Transform features
            var featurize = new Featurize()
                .SetOutputCol("features")
                .SetInputCols(featureColumns)
                .SetOneHotEncodeCategoricals(true)
                .SetNumFeatures(14);

            var dfTrans = featurize
                .Fit(df)
                .Transform(df)
                .WithColumn("label", Functions.When(Functions.Col("income").Contains("<"), 0.0).Otherwise(1.0));

            DataFrame[] dfs = dfTrans.RandomSplit(new double[] {0.75, 0.25}, 123);
            var trainDf = dfs[0];
            var testDf = dfs[1];

            // Create LightGBMClassifier
            var lightGBMClassifier = new LightGBMClassifier()
                .SetFeaturesCol("features")
                .SetRawPredictionCol("rawPrediction")
                .SetObjective("binary")
                .SetNumLeaves(30)
                .SetNumIterations(200)
                .SetLabelCol("label")
                .SetLeafPredictionCol("leafPrediction")
                .SetFeaturesShapCol("featuresShap");

            // Fit the model
            var lightGBMClassificationModel = lightGBMClassifier.Fit(trainDf);

            // Apply transformation and displayresults
            lightGBMClassificationModel.Transform(testDf).Show(50);

            // Stop Spark session
            spark.Stop();
        }
    }
}
```

Run `dotnet build` to build the project. Then navigate to build output directory, and run following command:
```powershell
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --packages com.microsoft.azure:synapseml_2.12:1.0.11,org.apache.hadoop:hadoop-azure:3.3.1 --master local microsoft-spark-3-2_2.12-2.1.1.jar dotnet SynapseMLApp.dll
```
:::note
Here we added two packages: synapseml_2.12 for SynapseML's scala source, and hadoop-azure to support reading files from ADLS.
:::

Expected output:
```
+---+---------+------+-------------+-------------+--------------+------------------+---------------+-------------------+-------+------------+------------+--------------+--------------+------+--------------------+-----+--------------------+--------------------+----------+--------------------+--------------------+
|age|workclass|fnlwgt|    education|education-num|marital-status|        occupation|   relationship|               race|    sex|capital-gain|capital-loss|hours-per-week|native-country|income|            features|label|       rawPrediction|         probability|prediction|      leafPrediction|        featuresShap|
+---+---------+------+-------------+-------------+--------------+------------------+---------------+-------------------+-------+------------+------------+--------------+--------------+------+--------------------+-----+--------------------+--------------------+----------+--------------------+--------------------+
| 17|        ?|634226|         10th|            6| Never-married|                 ?|      Own-child|              White| Female|           0|           0|          17.0| United-States| <=50K|(61,[7,9,11,15,20...|  0.0|[9.37122343731523...|[0.99991486808581...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.0560742274706...|
| 17|  Private| 73145|          9th|            5| Never-married|      Craft-repair|      Own-child|              White| Female|           0|           0|          16.0| United-States| <=50K|(61,[7,9,11,15,17...|  0.0|[12.7512760001880...|[0.99999710138899...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1657810433238...|
| 17|  Private|150106|         10th|            6| Never-married|             Sales|      Own-child|              White| Female|           0|           0|          20.0| United-States| <=50K|(61,[5,9,11,15,17...|  0.0|[12.7676985938038...|[0.99999714860282...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1276877355292...|
| 17|  Private|151141|         11th|            7| Never-married| Handlers-cleaners|      Own-child|              White|   Male|           0|           0|          15.0| United-States| <=50K|(61,[8,9,11,15,17...|  0.0|[12.1656242513070...|[0.99999479363924...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1279828578119...|
| 17|  Private|327127|         11th|            7| Never-married|  Transport-moving|      Own-child|              White|   Male|           0|           0|          20.0| United-States| <=50K|(61,[1,9,11,15,17...|  0.0|[12.9962776686392...|[0.99999773124636...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1164691543415...|
| 18|        ?|171088| Some-college|           10| Never-married|                 ?|      Own-child|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[7,9,11,15,20...|  0.0|[12.9400428266629...|[0.99999760000817...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1554829578661...|
| 18|  Private|115839|         12th|            8| Never-married|      Adm-clerical|  Not-in-family|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[11.8393032168619...|[0.99999278472630...|       0.0|[0.0,0.0,0.0,0.0,...|[0.44080835709189...|
| 18|  Private|133055|      HS-grad|            9| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[11.5747235180479...|[0.99999059936124...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1415862541824...|
| 18|  Private|169745|      7th-8th|            4| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[11.8316427733613...|[0.99999272924226...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1527378526573...|
| 18|  Private|177648|      HS-grad|            9| Never-married|             Sales|      Own-child|              White| Female|           0|           0|          25.0| United-States| <=50K|(61,[5,9,11,15,17...|  0.0|[10.0820248199174...|[0.99995817710510...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1151843103241...|
| 18|  Private|188241|         11th|            7| Never-married|     Other-service|      Own-child|              White|   Male|           0|           0|          16.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[10.4049945509280...|[0.99996972005153...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1356854966291...|
| 18|  Private|200603|      HS-grad|            9| Never-married|      Adm-clerical| Other-relative|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[12.1354343020828...|[0.99999463406365...|       0.0|[0.0,0.0,0.0,0.0,...|[0.53241098695335...|
| 18|  Private|210026|         10th|            6| Never-married|     Other-service| Other-relative|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[12.3692360082180...|[0.99999575275599...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1275208795564...|
| 18|  Private|447882| Some-college|           10| Never-married|      Adm-clerical|  Not-in-family|              White| Female|           0|           0|          20.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[10.2514945786032...|[0.99996469655062...|       0.0|[0.0,0.0,0.0,0.0,...|[0.36497782752201...|
| 19|        ?|242001| Some-college|           10| Never-married|                 ?|      Own-child|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[7,9,11,15,20...|  0.0|[13.9439986622060...|[0.99999912057674...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1265631737386...|
| 19|  Private| 63814| Some-college|           10| Never-married|      Adm-clerical|  Not-in-family|              White| Female|           0|           0|          18.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[10.2057742895673...|[0.99996304506073...|       0.0|[0.0,0.0,0.0,0.0,...|[0.77645146059597...|
| 19|  Private| 83930|      HS-grad|            9| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          20.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[10.4771335467356...|[0.99997182742919...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1625827100973...|
| 19|  Private| 86150|         11th|            7| Never-married|             Sales|      Own-child| Asian-Pac-Islander| Female|           0|           0|          19.0|   Philippines| <=50K|(61,[5,9,14,15,17...|  0.0|[12.0241839747799...|[0.99999400263272...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1532111483051...|
| 19|  Private|189574|      HS-grad|            9| Never-married|     Other-service|  Not-in-family|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[9.53742673004733...|[0.99992790305091...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.0988907054317...|
| 19|  Private|219742| Some-college|           10| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          15.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[12.8625329757574...|[0.99999740658642...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1922327651359...|
+---+---------+------+-------------+-------------+--------------+------------------+---------------+-------------------+-------+------------+------------+--------------+--------------+------+--------------------+-----+--------------------+--------------------+----------+--------------------+--------------------+
```
