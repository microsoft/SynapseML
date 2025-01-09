"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[6406],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>d});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),p=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=p(e.components);return a.createElement(l.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},u=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=p(n),d=r,h=u["".concat(l,".").concat(d)]||u[d]||m[d]||o;return n?a.createElement(h,i(i({ref:t},c),{},{components:n})):a.createElement(h,i({ref:t},c))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,i[1]=s;for(var p=2;p<o;p++)i[p]=n[p];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}u.displayName="MDXCreateElement"},66222:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>m,frontMatter:()=>o,metadata:()=>s,toc:()=>p});var a=n(83117),r=(n(67294),n(3905));const o={title:"Multivariate Anomaly Detection",hide_title:!0,status:"stable"},i="Recipe: Azure AI Services - Multivariate Anomaly Detection",s={unversionedId:"Explore Algorithms/AI Services/Multivariate Anomaly Detection",id:"version-1.0.8/Explore Algorithms/AI Services/Multivariate Anomaly Detection",title:"Multivariate Anomaly Detection",description:"This recipe shows how you can use SynapseML and Azure AI services on Apache Spark for multivariate anomaly detection. Multivariate anomaly detection allows for the detection of anomalies among many variables or time series, taking into account all the inter-correlations and dependencies between the different variables. In this scenario, we use SynapseML to train a model for multivariate anomaly detection using the Azure AI services, and we then use to the model to infer multivariate anomalies within a dataset containing synthetic measurements from three IoT sensors.",source:"@site/versioned_docs/version-1.0.8/Explore Algorithms/AI Services/Multivariate Anomaly Detection.md",sourceDirName:"Explore Algorithms/AI Services",slug:"/Explore Algorithms/AI Services/Multivariate Anomaly Detection",permalink:"/SynapseML/docs/1.0.8/Explore Algorithms/AI Services/Multivariate Anomaly Detection",draft:!1,tags:[],version:"1.0.8",frontMatter:{title:"Multivariate Anomaly Detection",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Geospatial Services",permalink:"/SynapseML/docs/1.0.8/Explore Algorithms/AI Services/Geospatial Services"},next:{title:"Advanced Usage - Async, Batching, and Multi-Key",permalink:"/SynapseML/docs/1.0.8/Explore Algorithms/AI Services/Advanced Usage - Async, Batching, and Multi-Key"}},l={},p=[{value:"Important",id:"important",level:2},{value:"Setup",id:"setup",level:2},{value:"Create an Anomaly Detector resource",id:"create-an-anomaly-detector-resource",level:3},{value:"Create a Storage Account resource",id:"create-a-storage-account-resource",level:3},{value:"Enter your service keys",id:"enter-your-service-keys",level:3}],c={toc:p};function m(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"recipe-azure-ai-services---multivariate-anomaly-detection"},"Recipe: Azure AI Services - Multivariate Anomaly Detection"),(0,r.kt)("p",null,"This recipe shows how you can use SynapseML and Azure AI services on Apache Spark for multivariate anomaly detection. Multivariate anomaly detection allows for the detection of anomalies among many variables or time series, taking into account all the inter-correlations and dependencies between the different variables. In this scenario, we use SynapseML to train a model for multivariate anomaly detection using the Azure AI services, and we then use to the model to infer multivariate anomalies within a dataset containing synthetic measurements from three IoT sensors."),(0,r.kt)("p",null,"To learn more about the Azure AI Anomaly Detector, refer to ",(0,r.kt)("a",{parentName:"p",href:"https://docs.microsoft.com/azure/ai-services/anomaly-detector/"},"this documentation page"),". "),(0,r.kt)("h2",{id:"important"},"Important"),(0,r.kt)("p",null,"Starting on the 20th of September, 2023 you won\u2019t be able to create new Anomaly Detector resources. The Anomaly Detector service is being retired on the 1st of October, 2026."),(0,r.kt)("h2",{id:"setup"},"Setup"),(0,r.kt)("h3",{id:"create-an-anomaly-detector-resource"},"Create an Anomaly Detector resource"),(0,r.kt)("p",null,"Follow the instructions to create an ",(0,r.kt)("inlineCode",{parentName:"p"},"Anomaly Detector")," resource using the Azure portal or alternatively, you can also use the Azure CLI to create this resource."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"In the Azure portal, select ",(0,r.kt)("strong",{parentName:"li"},"Create")," in your resource group, and then type ",(0,r.kt)("strong",{parentName:"li"},"Anomaly Detector"),". Select the Anomaly Detector resource."),(0,r.kt)("li",{parentName:"ul"},"Give the resource a name, and ideally use the same region as the rest of your resource group. Use the default options for the rest, and then select ",(0,r.kt)("strong",{parentName:"li"},"Review + Create")," and then ",(0,r.kt)("strong",{parentName:"li"},"Create"),"."),(0,r.kt)("li",{parentName:"ul"},"Once the Anomaly Detector resource is created, open it and select the ",(0,r.kt)("inlineCode",{parentName:"li"},"Keys and Endpoints")," panel in the left nav. Copy the key for the Anomaly Detector resource into the ",(0,r.kt)("inlineCode",{parentName:"li"},"ANOMALY_API_KEY")," environment variable, or store it in the ",(0,r.kt)("inlineCode",{parentName:"li"},"anomalyKey")," variable.")),(0,r.kt)("h3",{id:"create-a-storage-account-resource"},"Create a Storage Account resource"),(0,r.kt)("p",null,"In order to save intermediate data, you need to create an Azure Blob Storage Account. Within that storage account, create a container for storing the intermediate data. Make note of the container name, and copy the connection string to that container. You need it later to populate the ",(0,r.kt)("inlineCode",{parentName:"p"},"containerName")," variable and the ",(0,r.kt)("inlineCode",{parentName:"p"},"BLOB_CONNECTION_STRING")," environment variable."),(0,r.kt)("h3",{id:"enter-your-service-keys"},"Enter your service keys"),(0,r.kt)("p",null,"Let's start by setting up the environment variables for our service keys. The next cell sets the ",(0,r.kt)("inlineCode",{parentName:"p"},"ANOMALY_API_KEY")," and the ",(0,r.kt)("inlineCode",{parentName:"p"},"BLOB_CONNECTION_STRING")," environment variables based on the values stored in our Azure Key Vault. If you're running this tutorial in your own environment, make sure you set these environment variables before you proceed."),(0,r.kt)("p",null,"Now, lets read the ",(0,r.kt)("inlineCode",{parentName:"p"},"ANOMALY_API_KEY")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"BLOB_CONNECTION_STRING")," environment variables and set the ",(0,r.kt)("inlineCode",{parentName:"p"},"containerName")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"location")," variables."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.core.platform import find_secret\n\n# An Anomaly Dectector subscription key\nanomalyKey = find_secret(\n    secret_name="anomaly-api-key", keyvault="mmlspark-build-keys"\n)  # use your own anomaly api key\n# Your storage account name\nstorageName = "anomalydetectiontest"  # use your own storage account name\n# A connection string to your blob storage account\nstorageKey = find_secret(\n    secret_name="madtest-storage-key", keyvault="mmlspark-build-keys"\n)  # use your own storage key\n# A place to save intermediate MVAD results\nintermediateSaveDir = (\n    "wasbs://madtest@anomalydetectiontest.blob.core.windows.net/intermediateData"\n)\n# The location of the anomaly detector resource that you created\nlocation = "westus2"\n')),(0,r.kt)("p",null,"First we connect to our storage account so that anomaly detector can save intermediate results there:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'spark.sparkContext._jsc.hadoopConfiguration().set(\n    f"fs.azure.account.key.{storageName}.blob.core.windows.net", storageKey\n)\n')),(0,r.kt)("p",null,"Let's import all the necessary modules."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"import numpy as np\nimport pandas as pd\n\nimport pyspark\nfrom pyspark.sql.functions import col\nfrom pyspark.sql.functions import lit\nfrom pyspark.sql.types import DoubleType\nimport matplotlib.pyplot as plt\n\nimport synapse.ml\nfrom synapse.ml.services.anomaly import *\n")),(0,r.kt)("p",null,"Now, let's read our sample data into a Spark DataFrame."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'df = (\n    spark.read.format("csv")\n    .option("header", "true")\n    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/MVAD/sample.csv")\n)\n\ndf = (\n    df.withColumn("sensor_1", col("sensor_1").cast(DoubleType()))\n    .withColumn("sensor_2", col("sensor_2").cast(DoubleType()))\n    .withColumn("sensor_3", col("sensor_3").cast(DoubleType()))\n)\n\n# Let\'s inspect the dataframe:\ndf.show(5)\n')),(0,r.kt)("p",null,"We can now create an ",(0,r.kt)("inlineCode",{parentName:"p"},"estimator")," object, which is used to train our model. We specify the start and end times for the training data. We also specify the input columns to use, and the name of the column that contains the timestamps. Finally, we specify the number of data points to use in the anomaly detection sliding window, and we set the connection string to the Azure Blob Storage Account. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'trainingStartTime = "2020-06-01T12:00:00Z"\ntrainingEndTime = "2020-07-02T17:55:00Z"\ntimestampColumn = "timestamp"\ninputColumns = ["sensor_1", "sensor_2", "sensor_3"]\n\nestimator = (\n    SimpleFitMultivariateAnomaly()\n    .setSubscriptionKey(anomalyKey)\n    .setLocation(location)\n    .setStartTime(trainingStartTime)\n    .setEndTime(trainingEndTime)\n    .setIntermediateSaveDir(intermediateSaveDir)\n    .setTimestampCol(timestampColumn)\n    .setInputCols(inputColumns)\n    .setSlidingWindow(200)\n)\n')),(0,r.kt)("p",null,"Now that we created the ",(0,r.kt)("inlineCode",{parentName:"p"},"estimator"),", let's fit it to the data:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"model = estimator.fit(df)\n")),(0,r.kt)("p",null,"Once the training is done, we can now use the model for inference. The code in the next cell specifies the start and end times for the data we would like to detect the anomalies in. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'inferenceStartTime = "2020-07-02T18:00:00Z"\ninferenceEndTime = "2020-07-06T05:15:00Z"\n\nresult = (\n    model.setStartTime(inferenceStartTime)\n    .setEndTime(inferenceEndTime)\n    .setOutputCol("results")\n    .setErrorCol("errors")\n    .setInputCols(inputColumns)\n    .setTimestampCol(timestampColumn)\n    .transform(df)\n)\n\nresult.show(5)\n')),(0,r.kt)("p",null,"When we called ",(0,r.kt)("inlineCode",{parentName:"p"},".show(5)")," in the previous cell, it showed us the first five rows in the dataframe. The results were all ",(0,r.kt)("inlineCode",{parentName:"p"},"null")," because they weren't inside the inference window."),(0,r.kt)("p",null,"To show the results only for the inferred data, lets select the columns we need. We can then order the rows in the dataframe by ascending order, and filter the result to only show the rows that are in the range of the inference window. In our case ",(0,r.kt)("inlineCode",{parentName:"p"},"inferenceEndTime")," is the same as the last row in the dataframe, so can ignore that. "),(0,r.kt)("p",null,"Finally, to be able to better plot the results, lets convert the Spark dataframe to a Pandas dataframe."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'rdf = (\n    result.select(\n        "timestamp",\n        *inputColumns,\n        "results.interpretation",\n        "isAnomaly",\n        "results.severity"\n    )\n    .orderBy("timestamp", ascending=True)\n    .filter(col("timestamp") >= lit(inferenceStartTime))\n    .toPandas()\n)\n\nrdf\n')),(0,r.kt)("p",null,"Format the ",(0,r.kt)("inlineCode",{parentName:"p"},"contributors")," column that stores the contribution score from each sensor to the detected anomalies. The next cell formats this data, and splits the contribution score of each sensor into its own column."),(0,r.kt)("p",null,"For Spark3.3 and below versions, the output of select statements will be in the format of ",(0,r.kt)("inlineCode",{parentName:"p"},"List<Rows>"),", so to format the data into dictionary and generate the values when interpretation is empty, please use the below parse method:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'def parse(x):\n    if len(x) > 0:\n        return dict([item[:2] for item in x])\n    else:\n        return {"sensor_1": 0, "sensor_2": 0, "sensor_3": 0}\n')),(0,r.kt)("p",null,"Staring with Spark3.4, the output of the select statement is already formatted as a ",(0,r.kt)("inlineCode",{parentName:"p"},"numpy.ndarry<dictionary>")," and no need to format the data again, so please use below parse method to generate the values when interpretation is empty:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'def parse(x):\n    if len(x) == 0:\n        return {"sensor_1": 0, "sensor_2": 0, "sensor_3": 0}\n\n\nrdf["contributors"] = rdf["interpretation"].apply(parse)\nrdf = pd.concat(\n    [\n        rdf.drop(["contributors"], axis=1),\n        pd.json_normalize(rdf["contributors"]).rename(\n            columns={\n                "sensor_1": "series_1",\n                "sensor_2": "series_2",\n                "sensor_3": "series_3",\n            }\n        ),\n    ],\n    axis=1,\n)\nrdf\n')),(0,r.kt)("p",null,"Great! We now have the contribution scores of sensors 1, 2, and 3 in the ",(0,r.kt)("inlineCode",{parentName:"p"},"series_0"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"series_1"),", and ",(0,r.kt)("inlineCode",{parentName:"p"},"series_2")," columns respectively. "),(0,r.kt)("p",null,"Run the next cell to plot the results. The ",(0,r.kt)("inlineCode",{parentName:"p"},"minSeverity")," parameter specifies the minimum severity of the anomalies to be plotted."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'minSeverity = 0.1\n\n\n####### Main Figure #######\nplt.figure(figsize=(23, 8))\nplt.plot(\n    rdf["timestamp"],\n    rdf["sensor_1"],\n    color="tab:orange",\n    line,\n    linewidth=2,\n    label="sensor_1",\n)\nplt.plot(\n    rdf["timestamp"],\n    rdf["sensor_2"],\n    color="tab:green",\n    line,\n    linewidth=2,\n    label="sensor_2",\n)\nplt.plot(\n    rdf["timestamp"],\n    rdf["sensor_3"],\n    color="tab:blue",\n    line,\n    linewidth=2,\n    label="sensor_3",\n)\nplt.grid(axis="y")\nplt.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\nplt.legend()\n\nanoms = list(rdf["severity"] >= minSeverity)\n_, _, ymin, ymax = plt.axis()\nplt.vlines(np.where(anoms), ymin=ymin, ymax=ymax, color="r", alpha=0.8)\n\nplt.legend()\nplt.title(\n    "A plot of the values from the three sensors with the detected anomalies highlighted in red."\n)\nplt.show()\n\n####### Severity Figure #######\nplt.figure(figsize=(23, 1))\nplt.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\nplt.plot(\n    rdf["timestamp"],\n    rdf["severity"],\n    color="black",\n    line,\n    linewidth=2,\n    label="Severity score",\n)\nplt.plot(\n    rdf["timestamp"],\n    [minSeverity] * len(rdf["severity"]),\n    color="red",\n    line,\n    linewidth=1,\n    label="minSeverity",\n)\nplt.grid(axis="y")\nplt.legend()\nplt.ylim([0, 1])\nplt.title("Severity of the detected anomalies")\nplt.show()\n\n####### Contributors Figure #######\nplt.figure(figsize=(23, 1))\nplt.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\nplt.bar(\n    rdf["timestamp"], rdf["series_1"], width=2, color="tab:orange", label="sensor_1"\n)\nplt.bar(\n    rdf["timestamp"],\n    rdf["series_2"],\n    width=2,\n    color="tab:green",\n    label="sensor_2",\n    bottom=rdf["series_1"],\n)\nplt.bar(\n    rdf["timestamp"],\n    rdf["series_3"],\n    width=2,\n    color="tab:blue",\n    label="sensor_3",\n    bottom=rdf["series_1"] + rdf["series_2"],\n)\nplt.grid(axis="y")\nplt.legend()\nplt.ylim([0, 1])\nplt.title("The contribution of each sensor to the detected anomaly")\nplt.show()\n')),(0,r.kt)("img",{width:"1300",src:"https://mmlspark.blob.core.windows.net/graphics/multivariate-anomaly-detection-plot.png"}),(0,r.kt)("p",null,"The plots show the raw data from the sensors (inside the inference window) in orange, green, and blue. The red vertical lines in the first figure show the detected anomalies that have a severity greater than or equal to ",(0,r.kt)("inlineCode",{parentName:"p"},"minSeverity"),". "),(0,r.kt)("p",null,"The second plot shows the severity score of all the detected anomalies, with the ",(0,r.kt)("inlineCode",{parentName:"p"},"minSeverity")," threshold shown in the dotted red line."),(0,r.kt)("p",null,"Finally, the last plot shows the contribution of the data from each sensor to the detected anomalies. It helps us diagnose and understand the most likely cause of each anomaly."))}m.isMDXComponent=!0}}]);