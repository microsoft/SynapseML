---
title: .NET setup
hide_title: true
sidebar_label: .NET setup
description: .NET setup and example for SynapseML
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# .NET setup and example for SynapseML

## Installation

### 1. Install .NET

To start building .NET apps, you need to download and install the .NET SDK (Software Development Kit).

Download and install the [.NET Core SDK](https://dotnet.microsoft.com/en-us/download/dotnet/3.1).
Installing the SDK adds the dotnet toolchain to your PATH.

Once you've installed the .NET Core SDK, open a new command prompt or terminal and run `dotnet`.

If the command runs and prints out information about how to use dotnet, can move to the next step.
If you receive a `'dotnet' is not recognized as an internal or external command` error, make sure
you opened a new command prompt or terminal before running the command.

### 2. Install Java

Install [Java 8.1](https://www.oracle.com/java/technologies/downloads/#java8) for Windows and macOS,
or [OpenJDK 8](https://openjdk.org/install/) for Ubuntu.

Select the appropriate version for your operating system. For example, select jdk-8u201-windows-x64.exe
for a Windows x64 machine or jdk-8u231-macosx-x64.dmg for macOS. Then, use the command java to verify the installation.

### 3. Install Apache Spark

[Download and install Apache Spark](https://spark.apache.org/downloads.html) with version >= 3.2.0.
(SynapseML v0.10.0 only supports spark version >= 3.2.0)

Extract downloaded zipped files (with 7-Zip app on Windows or `tar` on linux) and remember the location of
extracted files, we take `~/bin/spark-3.2.0-bin-hadoop3.2/` as an example here.

Run the following commands to set the environment variables used to locate Apache Spark.
On Windows, make sure to run the command prompt in administrator mode.
<Tabs groupId="operating-systems">
   <TabItem value="win" label="Windows" default>

      setx /M HADOOP_HOME C:\bin\spark-3.2.0-bin-hadoop3.2\
      setx /M SPARK_HOME C:\bin\spark-3.2.0-bin-hadoop3.2\
      setx /M PATH "%PATH%;%HADOOP_HOME%;%SPARK_HOME%bin" # Warning: Don't run this if your path is already long as it will truncate your path to 1024 characters and potentially remove entries!

   </TabItem>
   <TabItem value="linux" label="Mac/Linux">

      export SPARK_HOME=~/bin/spark-3.2.0-bin-hadoop3.2/
      export PATH="$SPARK_HOME/bin:$PATH"
      source ~/.bashrc

   </TabItem>
</Tabs>

Once you've installed everything and set your environment variables, open a **new** command prompt or terminal and run the following command:
```bash
spark-submit --version
```
If the command runs and prints version information, you can move to the next step.

If you receive a `'spark-submit' is not recognized as an internal or external command` error, make sure you opened a **new** command prompt.

### 4. Install .NET for Apache Spark

Download the [Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases) **v2.1.1** release from the .NET for Apache Spark GitHub.
For example if you're on a Windows machine and plan to use .NET Core, download the Windows x64 netcoreapp3.1 release.

Extract Microsoft.Spark.Worker and remember the location.

### 5. Install WinUtils (Windows Only)

.NET for Apache Spark requires WinUtils to be installed alongside Apache Spark.
[Download winutils.exe](https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe).
Then, copy WinUtils into C:\bin\spark-3.2.0-bin-hadoop3.2\bin.
:::note
If you are using a different version of Hadoop, which is annotated at the end of your Spark install folder name, select the version of WinUtils that's compatible with your version of Hadoop.
:::

### 6. Set DOTNET_WORKER_DIR and check dependencies

Run one of the following commands to set the DOTNET_WORKER_DIR environment variable, which is used by .NET apps to locate .NET for Apache Spark
worker binaries. Make sure to replace <PATH-DOTNET_WORKER_DIR> with the directory where you downloaded and extracted the Microsoft.Spark.Worker.
On Windows, make sure to run the command prompt in administrator mode.

<Tabs groupId="operating-systems">
   <TabItem value="win" label="Windows" default>

      setx /M DOTNET_WORKER_DIR <PATH-DOTNET-WORKER-DIR>

   </TabItem>
   <TabItem value="linux" label="Mac/Linux">

      export DOTNET_WORKER_DIR=<PATH-DOTNET-WORKER-DIR>

   </TabItem>
</Tabs>

Finally, double-check that you can run `dotnet, java, spark-shell` from your command line before you move to the next section.

## Write a .NET for SynapseML App

### 1. Create a console app

In your command prompt or terminal, run the following commands to create a new console application:
```powershell
dotnet new console -o SynapseMLApp
cd SynapseMLApp
```
The `dotnet` command creates a new application of type console for you. The -o parameter creates a directory
named `SynapseMLApp` where your app is stored and populates it with the required files.
The `cd SynapseMLApp` command changes the directory to the app directory you created.

### 2. Install Nuget package

To use .NET for Apache Spark in an app, install the Microsoft.Spark package.
In your command prompt or terminal, run the following command:
```powershell
dotnet add package Microsoft.Spark --version 2.1.1
```
:::note
This tutorial uses Microsoft.Spark 2.1.1 version as SynapseML 0.10.0 depends on it.
Change to corresponding version if necessary.
:::

To use SynapseML features in the app, install SynapseML.X package.
In this tutorial, we use SynapseML.Cognitive as an example.
In your command prompt or terminal, run the following command:
```powershell
# Update Nuget Config to include SynapseML Feed
dotnet nuget add source https://mmlspark.blob.core.windows.net/synapsemlnuget/index.json -n SynapseMLFeed
dotnet add package SynapseML.Cognitive --version 0.10.0
```
The `dotnet nuget add` command adds SynapseML's resolver to the source, so that our package can be found.

### 3. Write your app
Open Program.cs file in Visual Studio Code, or any text editor, and replace all of the code with the following:
```csharp
using System;
using System.Collections.Generic;
using Synapse.ML.Cognitive;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace SynapseMLApp
{
    class Program
    {        static void Main(string[] args)
        {
            // Create Spark session
            SparkSession spark =
                SparkSession
                    .Builder()
                    .AppName("TextSentimentExample")
                    .GetOrCreate();

            // Create DataFrame
            DataFrame df = spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {"I am so happy today, its sunny!", "en-US"}),
                    new GenericRow(new object[] {"I am frustrated by this rush hour traffic", "en-US"}),
                    new GenericRow(new object[] {"The cognitive services on spark aint bad", "en-US"})
                },
                new StructType(new List<StructField>
                {
                    new StructField("text", new StringType()),
                    new StructField("language", new StringType())
                })
            );

            // Create TextSentiment
            var model = new TextSentiment()
                .SetSubscriptionKey("YOUR_SUBSCRIPTION_KEY")
                .SetLocation("eastus")
                .SetTextCol("text")
                .SetOutputCol("sentiment")
                .SetErrorCol("error")
                .SetLanguageCol("language");

            // Transform
            var outputDF = model.Transform(df);

            // Display results
            outputDF.Show();

            // Stop Spark session
            spark.Stop();
        }
    }
}
```
[SparkSession](https://docs.microsoft.com/en-us/dotnet/api/microsoft.spark.sql.sparksession?view=spark-dotnet) is the entrypoint
of Apache Spark applications, which manages the context and information of your application. A DataFrame is a way of organizing
data into a set of named columns.

Create a [TextSentiment](https://mmlspark.blob.core.windows.net/docs/0.10.0/dotnet/classSynapse_1_1ML_1_1Cognitive_1_1TextSentiment.html)
instance, set corresponding subscription key and other configurations. Then, apply transformation to the dataframe,
which analyzes the sentiment based on each row, and stores result into output column.

The result of the transformation is stored in another DataFrame. Note that at this point, no operations have taken place because
.NET for Apache Spark lazily evaluates the data. It's not until the Show method is called to display the contents of the words
transformed DataFrame to the console that the operations defined in the lines above execute. Once you no longer need the Spark
session, use the Stop method to stop your session.

### 4. Run your .NET App
Run the following command to build your application:
```powershell
dotnet build
```
Navigate to your build output directory (In windows for example you could run `cd bin\Debug\net5.0`).
Use the spark-submit command to submit your application to run on Apache Spark.
```powershell
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --packages com.microsoft.azure:synapseml_2.12:0.10.0 --master local microsoft-spark-3-2_2.12-2.1.1.jar dotnet SynapseMLApp.dll
```
`--packages com.microsoft.azure:synapseml_2.12:0.10.0` specifies the dependency on synapseml_2.12 version 0.10.0;
`microsoft-spark-3-2_2.12-2.1.1.jar` specifies Microsoft.Spark version 2.1.1 and Spark version 3.2
:::note
This command assumes you have downloaded Apache Spark and added it to your PATH environment variable to be able to use spark-submit.
Otherwise, you'd have to use the full path (for example, C:\bin\apache-spark\bin\spark-submit or ~/spark/bin/spark-submit).
:::

When your app runs, the sentiment analysis result is written to the console.
```
+-----------------------------------------+--------+-----+--------------------------------------------------+
|                                     text|language|error|                                         sentiment|
+-----------------------------------------+--------+-----+--------------------------------------------------+
|          I am so happy today, its sunny!|   en-US| null|[{positive, null, {0.99, 0.0, 0.0}, [{I am so h...|
|I am frustrated by this rush hour traffic|   en-US| null|[{negative, null, {0.0, 0.0, 0.99}, [{I am frus...|
| The cognitive services on spark aint bad|   en-US| null|[{negative, null, {0.0, 0.01, 0.99}, [{The cogn...|
+-----------------------------------------+--------+-----+--------------------------------------------------+
```
Congratulations! You successfully authored and ran a .NET for SynapseML app.
Refer to the [developer docs](https://mmlspark.blob.core.windows.net/docs/0.10.0/dotnet/index.html) for API guidance.
