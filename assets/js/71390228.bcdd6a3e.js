"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[50020],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>m});var a=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},i=Object.keys(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var l=a.createContext({}),p=function(e){var t=a.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},c=function(e){var t=p(e.components);return a.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),d=p(r),m=n,f=d["".concat(l,".").concat(m)]||d[m]||u[m]||i;return r?a.createElement(f,o(o({ref:t},c),{},{components:r})):a.createElement(f,o({ref:t},c))}));function m(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=r.length,o=new Array(i);o[0]=d;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:n,o[1]=s;for(var p=2;p<i;p++)o[p]=r[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,r)}d.displayName="MDXCreateElement"},96546:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>u,frontMatter:()=>i,metadata:()=>s,toc:()=>p});var a=r(83117),n=(r(67294),r(3905));const i={title:"Quickstart - Classification using SparkML Vectors",hide_title:!0,status:"stable"},o=void 0,s={unversionedId:"Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using SparkML Vectors",id:"version-1.0.6/Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using SparkML Vectors",title:"Quickstart - Classification using SparkML Vectors",description:"SparkML Vector input",source:"@site/versioned_docs/version-1.0.6/Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using SparkML Vectors.md",sourceDirName:"Explore Algorithms/Vowpal Wabbit",slug:"/Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using SparkML Vectors",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using SparkML Vectors",draft:!1,tags:[],version:"1.0.6",frontMatter:{title:"Quickstart - Classification using SparkML Vectors",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Quickstart - Classification, Quantile Regression, and Regression",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Vowpal Wabbit/Quickstart - Classification, Quantile Regression, and Regression"},next:{title:"Quickstart - Classification using VW-native Format",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using VW-native Format"}},l={},p=[{value:"SparkML Vector input",id:"sparkml-vector-input",level:2},{value:"Read dataset",id:"read-dataset",level:4},{value:"Use VowpalWabbitFeaturizer to convert data features into vector",id:"use-vowpalwabbitfeaturizer-to-convert-data-features-into-vector",level:4},{value:"Split the dataset into train and test",id:"split-the-dataset-into-train-and-test",level:4},{value:"Model Training",id:"model-training",level:4},{value:"Model Prediction",id:"model-prediction",level:4}],c={toc:p};function u(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("img",{width:"200",src:"https://mmlspark.blob.core.windows.net/graphics/emails/vw-blue-dark-orange.svg"}),(0,n.kt)("h1",{id:"binary-classification-with-vowpalwabbit-on-criteo-dataset"},"Binary Classification with VowpalWabbit on Criteo Dataset"),(0,n.kt)("h2",{id:"sparkml-vector-input"},"SparkML Vector input"),(0,n.kt)("h4",{id:"read-dataset"},"Read dataset"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'import pyspark.sql.types as T\n\nschema = T.StructType(\n    [\n        T.StructField("label", T.IntegerType(), True),\n        *[T.StructField("i" + str(i), T.IntegerType(), True) for i in range(1, 13)],\n        *[T.StructField("s" + str(i), T.StringType(), True) for i in range(26)],\n    ]\n)\n\ndf = (\n    spark.read.format("csv")\n    .option("header", False)\n    .option("delimiter", "\\t")\n    .schema(schema)\n    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/criteo_day0_1k.csv.gz")\n)\n# print dataset basic info\nprint("records read: " + str(df.count()))\nprint("Schema: ")\ndf.printSchema()\n')),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},"display(df)\n")),(0,n.kt)("h4",{id:"use-vowpalwabbitfeaturizer-to-convert-data-features-into-vector"},"Use VowpalWabbitFeaturizer to convert data features into vector"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import VowpalWabbitFeaturizer\n\nfeaturizer = VowpalWabbitFeaturizer(\n    inputCols=[\n        *["i" + str(i) for i in range(1, 13)],\n        *["s" + str(i) for i in range(26)],\n    ],\n    outputCol="features",\n)\n\ndf = featurizer.transform(df).select("label", "features")\n')),(0,n.kt)("h4",{id:"split-the-dataset-into-train-and-test"},"Split the dataset into train and test"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},"train, test = df.randomSplit([0.85, 0.15], seed=1)\n")),(0,n.kt)("h4",{id:"model-training"},"Model Training"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import VowpalWabbitClassifier\n\nmodel = VowpalWabbitClassifier(\n    numPasses=20,\n    labelCol="label",\n    featuresCol="features",\n    passThroughArgs="--holdout_off --loss_function logistic",\n).fit(train)\n')),(0,n.kt)("h4",{id:"model-prediction"},"Model Prediction"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},"predictions = model.transform(test)\ndisplay(predictions)\n")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.train import ComputeModelStatistics\n\nmetrics = ComputeModelStatistics(\n    evaluationMetric="classification", labelCol="label", scoredLabelsCol="prediction"\n).transform(predictions)\ndisplay(metrics)\n')))}u.isMDXComponent=!0}}]);