"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[59284],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>f});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function s(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=r.createContext({}),c=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):s(s({},t),e)),n},p=function(e){var t=c(e.components);return r.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,l=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),m=c(n),f=a,d=m["".concat(l,".").concat(f)]||m[f]||u[f]||i;return n?r.createElement(d,s(s({ref:t},p),{},{components:n})):r.createElement(d,s({ref:t},p))}));function f(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,s=new Array(i);s[0]=m;var o={};for(var l in t)hasOwnProperty.call(t,l)&&(o[l]=t[l]);o.originalType=e,o.mdxType="string"==typeof e?e:a,s[1]=o;for(var c=2;c<i;c++)s[c]=n[c];return r.createElement.apply(null,s)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},3307:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>s,default:()=>u,frontMatter:()=>i,metadata:()=>o,toc:()=>c});var r=n(83117),a=(n(67294),n(3905));const i={title:"Quickstart - Train Classifier",hide_title:!0,status:"stable"},s=void 0,o={unversionedId:"Explore Algorithms/Classification/Quickstart - Train Classifier",id:"version-1.0.10/Explore Algorithms/Classification/Quickstart - Train Classifier",title:"Quickstart - Train Classifier",description:"Classification - Adult Census",source:"@site/versioned_docs/version-1.0.10/Explore Algorithms/Classification/Quickstart - Train Classifier.md",sourceDirName:"Explore Algorithms/Classification",slug:"/Explore Algorithms/Classification/Quickstart - Train Classifier",permalink:"/SynapseML/docs/1.0.10/Explore Algorithms/Classification/Quickstart - Train Classifier",draft:!1,tags:[],version:"1.0.10",frontMatter:{title:"Quickstart - Train Classifier",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Quickstart - Synthetic difference in differences",permalink:"/SynapseML/docs/1.0.10/Explore Algorithms/Causal Inference/Quickstart - Synthetic difference in differences"},next:{title:"Quickstart - SparkML vs SynapseML",permalink:"/SynapseML/docs/1.0.10/Explore Algorithms/Classification/Quickstart - SparkML vs SynapseML"}},l={},c=[{value:"Classification - Adult Census",id:"classification---adult-census",level:2}],p={toc:c};function u(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"classification---adult-census"},"Classification - Adult Census"),(0,a.kt)("p",null,"In this example, we try to predict incomes from the ",(0,a.kt)("em",{parentName:"p"},"Adult Census")," dataset."),(0,a.kt)("p",null,"First, we import the packages (use ",(0,a.kt)("inlineCode",{parentName:"p"},"help(synapse)")," to view contents),"),(0,a.kt)("p",null,"Now let's read the data and split it to train and test sets:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'data = spark.read.parquet(\n    "wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet"\n)\ndata = data.select(["education", "marital-status", "hours-per-week", "income"])\ntrain, test = data.randomSplit([0.75, 0.25], seed=123)\ntrain.limit(10).toPandas()\n')),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"TrainClassifier")," can be used to initialize and fit a model, it wraps SparkML classifiers.\nYou can use ",(0,a.kt)("inlineCode",{parentName:"p"},"help(synapse.ml.train.TrainClassifier)")," to view the different parameters."),(0,a.kt)("p",null,"Note that it implicitly converts the data into the format expected by the algorithm: tokenize\nand hash strings, one-hot encodes categorical variables, assembles the features into a vector\nand so on.  The parameter ",(0,a.kt)("inlineCode",{parentName:"p"},"numFeatures")," controls the number of hashed features."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.train import TrainClassifier\nfrom pyspark.ml.classification import LogisticRegression\n\nmodel = TrainClassifier(\n    model=LogisticRegression(), labelCol="income", numFeatures=256\n).fit(train)\n')),(0,a.kt)("p",null,"Finally, we save the model so it can be used in a scoring program."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.core.platform import *\n\nif running_on_synapse() or running_on_synapse_internal():\n    model.write().overwrite().save("Files/models/AdultCensus.mml")\nelif running_on_databricks():\n    model.write().overwrite().save("dbfs:/AdultCensus.mml")\nelif running_on_binder():\n    model.write().overwrite().save("/tmp/AdultCensus.mml")\nelse:\n    print(f"{current_platform()} platform not supported")\n')))}u.isMDXComponent=!0}}]);