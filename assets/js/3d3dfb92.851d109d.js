"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[72305],{3905:(e,n,t)=>{t.d(n,{Zo:()=>u,kt:()=>m});var a=t(67294);function r(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function l(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);n&&(a=a.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,a)}return t}function s(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?l(Object(t),!0).forEach((function(n){r(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):l(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function o(e,n){if(null==e)return{};var t,a,r=function(e,n){if(null==e)return{};var t,a,r={},l=Object.keys(e);for(a=0;a<l.length;a++)t=l[a],n.indexOf(t)>=0||(r[t]=e[t]);return r}(e,n);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)t=l[a],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var i=a.createContext({}),p=function(e){var n=a.useContext(i),t=n;return e&&(t="function"==typeof e?e(n):s(s({},n),e)),t},u=function(e){var n=p(e.components);return a.createElement(i.Provider,{value:n},e.children)},c={inlineCode:"code",wrapper:function(e){var n=e.children;return a.createElement(a.Fragment,{},n)}},d=a.forwardRef((function(e,n){var t=e.components,r=e.mdxType,l=e.originalType,i=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),d=p(t),m=r,f=d["".concat(i,".").concat(m)]||d[m]||c[m]||l;return t?a.createElement(f,s(s({ref:n},u),{},{components:t})):a.createElement(f,s({ref:n},u))}));function m(e,n){var t=arguments,r=n&&n.mdxType;if("string"==typeof e||r){var l=t.length,s=new Array(l);s[0]=d;var o={};for(var i in n)hasOwnProperty.call(n,i)&&(o[i]=n[i]);o.originalType=e,o.mdxType="string"==typeof e?e:r,s[1]=o;for(var p=2;p<l;p++)s[p]=t[p];return a.createElement.apply(null,s)}return a.createElement.apply(null,t)}d.displayName="MDXCreateElement"},6297:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>i,contentTitle:()=>s,default:()=>c,frontMatter:()=>l,metadata:()=>o,toc:()=>p});var a=t(83117),r=(t(67294),t(3905));const l={title:"R setup",hide_title:!0,sidebar_label:"R setup",description:"R setup and example for SynapseML"},s="R setup and example for SynapseML",o={unversionedId:"Reference/R Setup",id:"version-1.0.7/Reference/R Setup",title:"R setup",description:"R setup and example for SynapseML",source:"@site/versioned_docs/version-1.0.7/Reference/R Setup.md",sourceDirName:"Reference",slug:"/Reference/R Setup",permalink:"/SynapseML/docs/Reference/R Setup",draft:!1,tags:[],version:"1.0.7",frontMatter:{title:"R setup",hide_title:!0,sidebar_label:"R setup",description:"R setup and example for SynapseML"},sidebar:"docs",previous:{title:"Docker Setup",permalink:"/SynapseML/docs/Reference/Docker Setup"}},i={},p=[{value:"Installation",id:"installation",level:2},{value:"Importing libraries and setting up spark context",id:"importing-libraries-and-setting-up-spark-context",level:3},{value:"Example",id:"example",level:2},{value:"Azure Databricks",id:"azure-databricks",level:2},{value:"Building from Source",id:"building-from-source",level:2}],u={toc:p};function c(e){let{components:n,...t}=e;return(0,r.kt)("wrapper",(0,a.Z)({},u,t,{components:n,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"r-setup-and-example-for-synapseml"},"R setup and example for SynapseML"),(0,r.kt)("h2",{id:"installation"},"Installation"),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Requirements"),": Ensure that R and\n",(0,r.kt)("a",{parentName:"p",href:"https://github.com/hadley/devtools"},"devtools")," installed on your\nmachine."),(0,r.kt)("p",null,"Also make sure you have Apache Spark installed. If you are using Sparklyr, you can use ",(0,r.kt)("a",{parentName:"p",href:"https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_install.html"},"spark-install"),'. Be sure to specify the correct version. As of this writing, that should be version="3.2". spark_install is a bit eccentric and may install a slightly different version. Be sure that the version you get is one that you want.'),(0,r.kt)("p",null,"On Windows, download ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe"},"WinUtils.exe")," and copy it into the ",(0,r.kt)("inlineCode",{parentName:"p"},"bin")," directory of your Spark installation, e.g. C:\\Users\\user\\AppData\\Local\\Spark\\spark-3.3.2-bin-hadoop3\\bin"),(0,r.kt)("p",null,"To install the current SynapseML package for R, first install synapseml-core:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},'...\ndevtools::install_url("https://mmlspark.azureedge.net/rrr/synapseml-core-0.11.0.zip")\n...\n')),(0,r.kt)("p",null,"and then install any or all of the following packages, depending on your intended usage:"),(0,r.kt)("p",null,"synapseml-cognitive,\nsynapseml-deep-learning,\nsynapseml-lightgbm,\nsynapseml-opencv,\nsynapseml-vw"),(0,r.kt)("p",null,"In other words:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},'...\ndevtools::install_url("https://mmlspark.azureedge.net/rrr/synapseml-cognitive-0.11.0.zip")\ndevtools::install_url("https://mmlspark.azureedge.net/rrr/synapseml-deep-learning-0.11.0.zip")\ndevtools::install_url("https://mmlspark.azureedge.net/rrr/synapseml-lightgbm-0.11.0.zip")\ndevtools::install_url("https://mmlspark.azureedge.net/rrr/synapseml-opencv-0.11.0.zip")\ndevtools::install_url("https://mmlspark.azureedge.net/rrr/synapseml-vw-0.11.0.zip")\n...\n')),(0,r.kt)("h3",{id:"importing-libraries-and-setting-up-spark-context"},"Importing libraries and setting up spark context"),(0,r.kt)("p",null,"Installing all dependencies may be time-consuming.  When complete, run:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},'...\nlibrary(sparklyr)\nlibrary(dplyr)\nconfig <- spark_config()\nconfig$sparklyr.defaultPackages <- "com.microsoft.azure:synapseml_2.12:1.0.7"\nsc <- spark_connect(master = "local", config = config)\n...\n')),(0,r.kt)("p",null,"This creates a spark context on your local machine."),(0,r.kt)("p",null,"We then need to import the R wrappers:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},"...\n library(synapseml.core)\n library(synapseml.cognitive)\n library(synapseml.deep.learning)\n library(synapseml.lightgbm)\n library(synapseml.opencv)\n library(synapseml.vw)\n...\n")),(0,r.kt)("h2",{id:"example"},"Example"),(0,r.kt)("p",null,"We can use the faithful dataset in R:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},'...\nfaithful_df <- copy_to(sc, faithful)\ncmd_model = ml_clean_missing_data(\n              x=faithful_df,\n              inputCols = c("eruptions", "waiting"),\n              outputCols = c("eruptions_output", "waiting_output"),\n              only.model=TRUE)\nsdf_transform(cmd_model, faithful_df)\n...\n')),(0,r.kt)("p",null,"You should see the output:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},"...\n# Source:   table<sparklyr_tmp_17d66a9d490c> [?? x 4]\n# Database: spark_connection\n   eruptions waiting eruptions_output waiting_output\n          <dbl>   <dbl>            <dbl>          <dbl>\n          1     3.600      79            3.600             79\n          2     1.800      54            1.800             54\n          3     3.333      74            3.333             74\n          4     2.283      62            2.283             62\n          5     4.533      85            4.533             85\n          6     2.883      55            2.883             55\n          7     4.700      88            4.700             88\n          8     3.600      85            3.600             85\n          9     1.950      51            1.950             51\n          10     4.350      85            4.350             85\n          # ... with more rows\n...\n")),(0,r.kt)("h2",{id:"azure-databricks"},"Azure Databricks"),(0,r.kt)("p",null,'In Azure Databricks, you can install devtools and the spark package from URL\nand then use spark_connect with method = "databricks":'),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},'install.packages("devtools")\ndevtools::install_url("https://mmlspark.azureedge.net/rrr/synapseml-1.0.7.zip")\nlibrary(sparklyr)\nlibrary(dplyr)\nsc <- spark_connect(method = "databricks")\nfaithful_df <- copy_to(sc, faithful)\nunfit_model = ml_light_gbmregressor(sc, maxDepth=20, featuresCol="waiting", labelCol="eruptions", numIterations=10, unfit.model=TRUE)\nml_train_regressor(faithful_df, labelCol="eruptions", unfit_model)\n')),(0,r.kt)("h2",{id:"building-from-source"},"Building from Source"),(0,r.kt)("p",null,"Our R bindings are built as part of the ",(0,r.kt)("a",{parentName:"p",href:"../Developer%20Setup"},"normal build\nprocess"),".  To get a quick build, start at the root\nof the synapseml directory, and find the generated files. For instance,\nto find the R files for deep-learning, run"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"sbt packageR\nls ./deep-learning/target/scala-2.12/generated/src/R/synapseml/R\n")),(0,r.kt)("p",null,"You can then run R in a terminal and install the above files directly:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-R"},'...\ndevtools::install_local("./deep-learning/target/scala-2.12/generated/src/R/synapseml/R")\n...\n')))}c.isMDXComponent=!0}}]);