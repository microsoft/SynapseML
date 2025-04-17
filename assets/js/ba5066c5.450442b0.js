"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[88759],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>f});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),p=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},c=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,c=o(e,["components","mdxType","originalType","parentName"]),u=p(a),f=r,m=u["".concat(s,".").concat(f)]||u[f]||d[f]||i;return a?n.createElement(m,l(l({ref:t},c),{},{components:a})):n.createElement(m,l({ref:t},c))}));function f(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,l=new Array(i);l[0]=u;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:r,l[1]=o;for(var p=2;p<i;p++)l[p]=a[p];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}u.displayName="MDXCreateElement"},26710:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>d,frontMatter:()=>i,metadata:()=>o,toc:()=>p});var n=a(83117),r=(a(67294),a(3905));const i={title:"Multi-class classification",hide_title:!0,status:"stable"},l=void 0,o={unversionedId:"Explore Algorithms/Vowpal Wabbit/Multi-class classification",id:"version-v1.0.11/Explore Algorithms/Vowpal Wabbit/Multi-class classification",title:"Multi-class classification",description:"Read dataset",source:"@site/versioned_docs/version-v1.0.11/Explore Algorithms/Vowpal Wabbit/Multi-class classification.md",sourceDirName:"Explore Algorithms/Vowpal Wabbit",slug:"/Explore Algorithms/Vowpal Wabbit/Multi-class classification",permalink:"/SynapseML/docs/Explore Algorithms/Vowpal Wabbit/Multi-class classification",draft:!1,tags:[],version:"v1.0.11",frontMatter:{title:"Multi-class classification",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"About",permalink:"/SynapseML/docs/Explore Algorithms/Vowpal Wabbit/Overview"},next:{title:"Contextual Bandits",permalink:"/SynapseML/docs/Explore Algorithms/Vowpal Wabbit/Contextual Bandits"}},s={},p=[{value:"Read dataset",id:"read-dataset",level:4},{value:"Use VowpalWabbitFeaturizer to convert data features into vector",id:"use-vowpalwabbitfeaturizer-to-convert-data-features-into-vector",level:4},{value:"Split the dataset into train and test",id:"split-the-dataset-into-train-and-test",level:4},{value:"Model Training",id:"model-training",level:4},{value:"Model Prediction",id:"model-prediction",level:4}],c={toc:p};function d(e){let{components:t,...a}=e;return(0,r.kt)("wrapper",(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("img",{width:"200",src:"https://mmlspark.blob.core.windows.net/graphics/emails/vw-blue-dark-orange.svg"}),(0,r.kt)("h1",{id:"multi-class-classification-using-vowpal-wabbit"},"Multi-class Classification using Vowpal Wabbit"),(0,r.kt)("h4",{id:"read-dataset"},"Read dataset"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'import pyspark.sql.types as T\nfrom pyspark.sql import functions as F\n\nschema = T.StructType(\n    [\n        T.StructField("sepal_length", T.DoubleType(), False),\n        T.StructField("sepal_width", T.DoubleType(), False),\n        T.StructField("petal_length", T.DoubleType(), False),\n        T.StructField("petal_width", T.DoubleType(), False),\n        T.StructField("variety", T.StringType(), False),\n    ]\n)\n\ndf = (\n    spark.read.format("csv")\n    .option("header", True)\n    .schema(schema)\n    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/iris.txt")\n)\n# print dataset basic info\nprint("records read: " + str(df.count()))\nprint("Schema: ")\ndf.printSchema()\n')),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"display(df)\n")),(0,r.kt)("h4",{id:"use-vowpalwabbitfeaturizer-to-convert-data-features-into-vector"},"Use VowpalWabbitFeaturizer to convert data features into vector"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.ml.feature import StringIndexer\n\nfrom synapse.ml.vw import VowpalWabbitFeaturizer\n\nindexer = StringIndexer(inputCol="variety", outputCol="label")\nfeaturizer = VowpalWabbitFeaturizer(\n    inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],\n    outputCol="features",\n)\n\n# label needs to be integer (0 to n)\ndf_label = indexer.fit(df).transform(df).withColumn("label", F.col("label").cast("int"))\n\n# featurize data\ndf_featurized = featurizer.transform(df_label).select("label", "features")\n\ndisplay(df_featurized)\n')),(0,r.kt)("h4",{id:"split-the-dataset-into-train-and-test"},"Split the dataset into train and test"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"train, test = df_featurized.randomSplit([0.8, 0.2], seed=1)\n")),(0,r.kt)("h4",{id:"model-training"},"Model Training"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import VowpalWabbitClassifier\n\n\nmodel = (\n    VowpalWabbitClassifier(\n        numPasses=5,\n        passThroughArgs="--holdout_off --oaa 3 --holdout_off --loss_function=logistic --indexing 0 -q ::",\n    )\n    .setNumClasses(3)\n    .fit(train)\n)\n')),(0,r.kt)("h4",{id:"model-prediction"},"Model Prediction"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"predictions = model.transform(test)\n\ndisplay(predictions)\n")))}d.isMDXComponent=!0}}]);