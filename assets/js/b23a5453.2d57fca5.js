"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[33217],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>m});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},u=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=c(n),m=r,h=u["".concat(l,".").concat(m)]||u[m]||d[m]||i;return n?a.createElement(h,o(o({ref:t},p),{},{components:n})):a.createElement(h,o({ref:t},p))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var c=2;c<i;c++)o[c]=n[c];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}u.displayName="MDXCreateElement"},28942:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>s,toc:()=>c});var a=n(83117),r=(n(67294),n(3905));const i={title:"Quickstart - LightGBM in Dotnet",sidebar_label:"Quickstart - LightGBM in Dotnet",description:"A simple example about classification with LightGBMClassifier using .NET"},o=void 0,s={unversionedId:"Reference/Quickstart - LightGBM in Dotnet",id:"version-1.0.7/Reference/Quickstart - LightGBM in Dotnet",title:"Quickstart - LightGBM in Dotnet",description:"A simple example about classification with LightGBMClassifier using .NET",source:"@site/versioned_docs/version-1.0.7/Reference/Quickstart - LightGBM in Dotnet.md",sourceDirName:"Reference",slug:"/Reference/Quickstart - LightGBM in Dotnet",permalink:"/SynapseML/docs/1.0.7/Reference/Quickstart - LightGBM in Dotnet",draft:!1,tags:[],version:"1.0.7",frontMatter:{title:"Quickstart - LightGBM in Dotnet",sidebar_label:"Quickstart - LightGBM in Dotnet",description:"A simple example about classification with LightGBMClassifier using .NET"}},l={},c=[{value:"Classification with LightGBMClassifier",id:"classification-with-lightgbmclassifier",level:2}],p={toc:c};function d(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"Make sure you have followed the guidance in ",(0,r.kt)("a",{parentName:"p",href:"../Dotnet%20Setup"},".NET installation")," before jumping into this example.")),(0,r.kt)("h2",{id:"classification-with-lightgbmclassifier"},"Classification with LightGBMClassifier"),(0,r.kt)("p",null,"Install NuGet packages by running following command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-powershell"},"dotnet add package Microsoft.Spark --version 2.1.1\ndotnet add package SynapseML.Lightgbm --version 1.0.7\ndotnet add package SynapseML.Core --version 1.0.7\n")),(0,r.kt)("p",null,"Use the following code in your main program file:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-csharp"},'using System;\nusing System.Collections.Generic;\nusing Synapse.ML.Lightgbm;\nusing Synapse.ML.Featurize;\nusing Microsoft.Spark.Sql;\nusing Microsoft.Spark.Sql.Types;\n\nnamespace SynapseMLApp\n{\n    class Program\n    {\n        static void Main(string[] args)\n        {\n            // Create Spark session\n            SparkSession spark =\n                SparkSession\n                    .Builder()\n                    .AppName("LightGBMExample")\n                    .GetOrCreate();\n\n            // Load Data\n            DataFrame df = spark.Read()\n                .Option("inferSchema", true)\n                .Parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet")\n                .Limit(2000);\n\n            var featureColumns = new string[] {"age", "workclass", "fnlwgt", "education", "education-num",\n                "marital-status", "occupation", "relationship", "race", "sex", "capital-gain",\n                "capital-loss", "hours-per-week", "native-country"};\n\n            // Transform features\n            var featurize = new Featurize()\n                .SetOutputCol("features")\n                .SetInputCols(featureColumns)\n                .SetOneHotEncodeCategoricals(true)\n                .SetNumFeatures(14);\n\n            var dfTrans = featurize\n                .Fit(df)\n                .Transform(df)\n                .WithColumn("label", Functions.When(Functions.Col("income").Contains("<"), 0.0).Otherwise(1.0));\n\n            DataFrame[] dfs = dfTrans.RandomSplit(new double[] {0.75, 0.25}, 123);\n            var trainDf = dfs[0];\n            var testDf = dfs[1];\n\n            // Create LightGBMClassifier\n            var lightGBMClassifier = new LightGBMClassifier()\n                .SetFeaturesCol("features")\n                .SetRawPredictionCol("rawPrediction")\n                .SetObjective("binary")\n                .SetNumLeaves(30)\n                .SetNumIterations(200)\n                .SetLabelCol("label")\n                .SetLeafPredictionCol("leafPrediction")\n                .SetFeaturesShapCol("featuresShap");\n\n            // Fit the model\n            var lightGBMClassificationModel = lightGBMClassifier.Fit(trainDf);\n\n            // Apply transformation and displayresults\n            lightGBMClassificationModel.Transform(testDf).Show(50);\n\n            // Stop Spark session\n            spark.Stop();\n        }\n    }\n}\n')),(0,r.kt)("p",null,"Run ",(0,r.kt)("inlineCode",{parentName:"p"},"dotnet build")," to build the project. Then navigate to build output directory, and run following command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-powershell"},"spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --packages com.microsoft.azure:synapseml_2.12:1.0.7,org.apache.hadoop:hadoop-azure:3.3.1 --master local microsoft-spark-3-2_2.12-2.1.1.jar dotnet SynapseMLApp.dll\n")),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"Here we added two packages: synapseml_2.12 for SynapseML's scala source, and hadoop-azure to support reading files from ADLS.")),(0,r.kt)("p",null,"Expected output:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"+---+---------+------+-------------+-------------+--------------+------------------+---------------+-------------------+-------+------------+------------+--------------+--------------+------+--------------------+-----+--------------------+--------------------+----------+--------------------+--------------------+\n|age|workclass|fnlwgt|    education|education-num|marital-status|        occupation|   relationship|               race|    sex|capital-gain|capital-loss|hours-per-week|native-country|income|            features|label|       rawPrediction|         probability|prediction|      leafPrediction|        featuresShap|\n+---+---------+------+-------------+-------------+--------------+------------------+---------------+-------------------+-------+------------+------------+--------------+--------------+------+--------------------+-----+--------------------+--------------------+----------+--------------------+--------------------+\n| 17|        ?|634226|         10th|            6| Never-married|                 ?|      Own-child|              White| Female|           0|           0|          17.0| United-States| <=50K|(61,[7,9,11,15,20...|  0.0|[9.37122343731523...|[0.99991486808581...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.0560742274706...|\n| 17|  Private| 73145|          9th|            5| Never-married|      Craft-repair|      Own-child|              White| Female|           0|           0|          16.0| United-States| <=50K|(61,[7,9,11,15,17...|  0.0|[12.7512760001880...|[0.99999710138899...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1657810433238...|\n| 17|  Private|150106|         10th|            6| Never-married|             Sales|      Own-child|              White| Female|           0|           0|          20.0| United-States| <=50K|(61,[5,9,11,15,17...|  0.0|[12.7676985938038...|[0.99999714860282...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1276877355292...|\n| 17|  Private|151141|         11th|            7| Never-married| Handlers-cleaners|      Own-child|              White|   Male|           0|           0|          15.0| United-States| <=50K|(61,[8,9,11,15,17...|  0.0|[12.1656242513070...|[0.99999479363924...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1279828578119...|\n| 17|  Private|327127|         11th|            7| Never-married|  Transport-moving|      Own-child|              White|   Male|           0|           0|          20.0| United-States| <=50K|(61,[1,9,11,15,17...|  0.0|[12.9962776686392...|[0.99999773124636...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1164691543415...|\n| 18|        ?|171088| Some-college|           10| Never-married|                 ?|      Own-child|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[7,9,11,15,20...|  0.0|[12.9400428266629...|[0.99999760000817...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1554829578661...|\n| 18|  Private|115839|         12th|            8| Never-married|      Adm-clerical|  Not-in-family|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[11.8393032168619...|[0.99999278472630...|       0.0|[0.0,0.0,0.0,0.0,...|[0.44080835709189...|\n| 18|  Private|133055|      HS-grad|            9| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[11.5747235180479...|[0.99999059936124...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1415862541824...|\n| 18|  Private|169745|      7th-8th|            4| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[11.8316427733613...|[0.99999272924226...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1527378526573...|\n| 18|  Private|177648|      HS-grad|            9| Never-married|             Sales|      Own-child|              White| Female|           0|           0|          25.0| United-States| <=50K|(61,[5,9,11,15,17...|  0.0|[10.0820248199174...|[0.99995817710510...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1151843103241...|\n| 18|  Private|188241|         11th|            7| Never-married|     Other-service|      Own-child|              White|   Male|           0|           0|          16.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[10.4049945509280...|[0.99996972005153...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1356854966291...|\n| 18|  Private|200603|      HS-grad|            9| Never-married|      Adm-clerical| Other-relative|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[12.1354343020828...|[0.99999463406365...|       0.0|[0.0,0.0,0.0,0.0,...|[0.53241098695335...|\n| 18|  Private|210026|         10th|            6| Never-married|     Other-service| Other-relative|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[12.3692360082180...|[0.99999575275599...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1275208795564...|\n| 18|  Private|447882| Some-college|           10| Never-married|      Adm-clerical|  Not-in-family|              White| Female|           0|           0|          20.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[10.2514945786032...|[0.99996469655062...|       0.0|[0.0,0.0,0.0,0.0,...|[0.36497782752201...|\n| 19|        ?|242001| Some-college|           10| Never-married|                 ?|      Own-child|              White| Female|           0|           0|          40.0| United-States| <=50K|(61,[7,9,11,15,20...|  0.0|[13.9439986622060...|[0.99999912057674...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1265631737386...|\n| 19|  Private| 63814| Some-college|           10| Never-married|      Adm-clerical|  Not-in-family|              White| Female|           0|           0|          18.0| United-States| <=50K|(61,[0,9,11,15,17...|  0.0|[10.2057742895673...|[0.99996304506073...|       0.0|[0.0,0.0,0.0,0.0,...|[0.77645146059597...|\n| 19|  Private| 83930|      HS-grad|            9| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          20.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[10.4771335467356...|[0.99997182742919...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1625827100973...|\n| 19|  Private| 86150|         11th|            7| Never-married|             Sales|      Own-child| Asian-Pac-Islander| Female|           0|           0|          19.0|   Philippines| <=50K|(61,[5,9,14,15,17...|  0.0|[12.0241839747799...|[0.99999400263272...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1532111483051...|\n| 19|  Private|189574|      HS-grad|            9| Never-married|     Other-service|  Not-in-family|              White| Female|           0|           0|          30.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[9.53742673004733...|[0.99992790305091...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.0988907054317...|\n| 19|  Private|219742| Some-college|           10| Never-married|     Other-service|      Own-child|              White| Female|           0|           0|          15.0| United-States| <=50K|(61,[3,9,11,15,17...|  0.0|[12.8625329757574...|[0.99999740658642...|       0.0|[0.0,0.0,0.0,0.0,...|[-0.1922327651359...|\n+---+---------+------+-------------+-------------+--------------+------------------+---------------+-------------------+-------+------------+------------+--------------+--------------+------+--------------------+-----+--------------------+--------------------+----------+--------------------+--------------------+\n")))}d.isMDXComponent=!0}}]);