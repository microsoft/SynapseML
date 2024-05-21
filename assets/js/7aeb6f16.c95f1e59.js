"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[24046],{3905:(e,t,n)=>{n.d(t,{Zo:()=>f,kt:()=>d});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},f=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,f=s(e,["components","mdxType","originalType","parentName"]),m=c(n),d=r,u=m["".concat(l,".").concat(d)]||m[d]||p[d]||i;return n?a.createElement(u,o(o({ref:t},f),{},{components:n})):a.createElement(u,o({ref:t},f))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var c=2;c<i;c++)o[c]=n[c];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},99342:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>p,frontMatter:()=>i,metadata:()=>s,toc:()=>c});var a=n(83117),r=(n(67294),n(3905));const i={title:"Quickstart - Synthetic difference in differences",hide_title:!0,status:"stable"},o="Scalable Synthetic Difference in Differences",s={unversionedId:"Explore Algorithms/Causal Inference/Quickstart - Synthetic difference in differences",id:"version-1.0.4/Explore Algorithms/Causal Inference/Quickstart - Synthetic difference in differences",title:"Quickstart - Synthetic difference in differences",description:"This sample notebook aims to show readers how to use SynapseML's DiffInDiffEstimator, SyntheticControlEstimator and SyntheticDiffInDiffEstimator to estimate the causal effect of a treatment on a particular outcome.",source:"@site/versioned_docs/version-1.0.4/Explore Algorithms/Causal Inference/Quickstart - Synthetic difference in differences.md",sourceDirName:"Explore Algorithms/Causal Inference",slug:"/Explore Algorithms/Causal Inference/Quickstart - Synthetic difference in differences",permalink:"/SynapseML/docs/Explore Algorithms/Causal Inference/Quickstart - Synthetic difference in differences",draft:!1,tags:[],version:"1.0.4",frontMatter:{title:"Quickstart - Synthetic difference in differences",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Quickstart - Measure Heterogeneous Effects",permalink:"/SynapseML/docs/Explore Algorithms/Causal Inference/Quickstart - Measure Heterogeneous Effects"},next:{title:"Quickstart - Train Classifier",permalink:"/SynapseML/docs/Explore Algorithms/Classification/Quickstart - Train Classifier"}},l={},c=[],f={toc:c};function p(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},f,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"scalable-synthetic-difference-in-differences"},"Scalable Synthetic Difference in Differences"),(0,r.kt)("p",null,"This sample notebook aims to show readers how to use SynapseML's ",(0,r.kt)("inlineCode",{parentName:"p"},"DiffInDiffEstimator"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"SyntheticControlEstimator")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"SyntheticDiffInDiffEstimator")," to estimate the causal effect of a treatment on a particular outcome."),(0,r.kt)("p",null,"In this sample notebook, we will use the California smoking cessation program example to demonstrate usage of the SyntheticDiffInDiff Estimator. The goal of the analysis is to estimate the effect of increased cigarette taxes on smoking in California."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.sql.types import *\nfrom synapse.ml.causal import (\n    DiffInDiffEstimator,\n    SyntheticControlEstimator,\n    SyntheticDiffInDiffEstimator,\n)\nfrom matplotlib import pyplot as plt\nfrom matplotlib import style\nimport pandas as pd\nimport numpy as np\n\nspark.sparkContext.setLogLevel("INFO")\nstyle.use("ggplot")\n')),(0,r.kt)("p",null,"We will select 5 columns from the dataset: state, year, cigsale, california, after_treatment."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'df = (\n    spark.read.option("header", True)\n    .option("inferSchema", True)\n    .csv("wasbs://publicwasb@mmlspark.blob.core.windows.net/smoking.csv")\n    .select("state", "year", "cigsale", "california", "after_treatment")\n)\ndisplay(df)\n')),(0,r.kt)("p",null,"First, we use the ",(0,r.kt)("inlineCode",{parentName:"p"},"DiffInDiffEstimator"),' to estimate the causal effect with regular difference in differences method. We set the treatment indicator column to "california", set post-treatment indicator column to "after_treatment", and set the outcome column to "cigsale".'),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'estimator1 = DiffInDiffEstimator(\n    treatmentCol="california", postTreatmentCol="after_treatment", outcomeCol="cigsale"\n)\n\nmodel1 = estimator1.fit(df)\n\nprint("[Diff in Diff] treatment effect: {}".format(model1.treatmentEffect))\nprint("[Diff in Diff] standard error: {}".format(model1.standardError))\n')),(0,r.kt)("p",null,"The treatment effect estimated by difference in differences should be -27.349."),(0,r.kt)("p",null,"Next, we use ",(0,r.kt)("inlineCode",{parentName:"p"},"SyntheticControlEstimator")," to synthesize a control unit and use the synthetic control to estimate the causal effect. To create the synthetic control unit, we need to set the column which indicates the time when each outcome is measured, and the column which indicates the unit for which the outcome is measured."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'estimator2 = SyntheticControlEstimator(\n    timeCol="year",\n    unitCol="state",\n    treatmentCol="california",\n    postTreatmentCol="after_treatment",\n    outcomeCol="cigsale",\n    maxIter=5000,\n    numIterNoChange=50,\n    tol=1e-4,\n    stepSize=1.0,\n)\n\nmodel2 = estimator2.fit(df)\n\nprint("[Synthetic Control] treatment effect: {}".format(model2.treatmentEffect))\nprint("[Synthetic Control] standard error: {}".format(model2.standardError))\n')),(0,r.kt)("p",null,"The treatment effect estimated by synthetic control should be about -19.354."),(0,r.kt)("p",null,"Internally, a constrained least square regression is used to solve the unit weights for the synthetic control, and we can plot the loss history."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'lossHistory = pd.Series(np.array(model2.lossHistoryUnitWeights))\n\nplt.plot(lossHistory[2000:])\nplt.title("loss history - unit weights")\nplt.xlabel("Iteration")\nplt.ylabel("Loss")\nplt.show()\n\nprint("Mimimal loss: {}".format(lossHistory.min()))\n')),(0,r.kt)("p",null,"We can also visualize the synthetic control and compare it with the treated unit."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'sc_weights = model2.unitWeights.toPandas().set_index("state")\npdf = df.toPandas()\nsc = (\n    pdf.query("~california")\n    .pivot(index="year", columns="state", values="cigsale")\n    .dot(sc_weights)\n)\nplt.plot(sc, label="Synthetic Control")\nplt.plot(sc.index, pdf.query("california")["cigsale"], label="California", color="C1")\n\nplt.title("Synthetic Control Estimation")\nplt.ylabel("Cigarette Sales")\nplt.vlines(\n    x=1988,\n    ymin=40,\n    ymax=140,\n    line,\n    lw=2,\n    label="Proposition 99",\n    color="black",\n)\nplt.legend()\n')),(0,r.kt)("p",null,"Lastly, we use ",(0,r.kt)("inlineCode",{parentName:"p"},"SyntheticDiffInDiffEstimator")," to estimate the causal effect."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'estimator3 = SyntheticDiffInDiffEstimator(\n    timeCol="year",\n    unitCol="state",\n    treatmentCol="california",\n    postTreatmentCol="after_treatment",\n    outcomeCol="cigsale",\n    maxIter=5000,\n    numIterNoChange=50,\n    tol=1e-4,\n    stepSize=1.0,\n)\n\nmodel3 = estimator3.fit(df)\n\nprint("[Synthetic Diff in Diff] treatment effect: {}".format(model3.treatmentEffect))\nprint("[Synthetic Diff in Diff] standard error: {}".format(model3.standardError))\n')),(0,r.kt)("p",null,"The treatment effect estimated by synthetic control should be about -15.554."),(0,r.kt)("p",null,"Again, we can plot the loss history from the optimizer used to solve the unit weights and the time weights."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'lossHistory = pd.Series(np.array(model3.lossHistoryUnitWeights))\n\nplt.plot(lossHistory[1000:])\nplt.title("loss history - unit weights")\nplt.xlabel("Iteration")\nplt.ylabel("Loss")\nplt.show()\n\nprint("Mimimal loss: {}".format(lossHistory.min()))\n')),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'lossHistory = pd.Series(np.array(model3.lossHistoryTimeWeights))\n\nplt.plot(lossHistory[1000:])\nplt.title("loss history - time weights")\nplt.xlabel("Iteration")\nplt.ylabel("Loss")\nplt.show()\n\nprint("Mimimal loss: {}".format(lossHistory.min()))\n')),(0,r.kt)("p",null,"Here we plot the synthetic diff in diff estimate together with the time weights."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'unit_weights = model3.unitWeights.toPandas().set_index("state")\nunit_intercept = model3.unitIntercept\n\ntime_weights = model3.timeWeights.toPandas().set_index("year")\ntime_intercept = model3.timeIntercept\n\npdf = df.toPandas()\npivot_df_control = pdf.query("~california").pivot(\n    index="year", columns="state", values="cigsale"\n)\npivot_df_treat = pdf.query("california").pivot(\n    index="year", columns="state", values="cigsale"\n)\nsc_did = pivot_df_control.values @ unit_weights.values\ntreated_mean = pivot_df_treat.mean(axis=1)\n')),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'fig, (ax1, ax2) = plt.subplots(\n    2, 1, figsize=(15, 8), sharex=True, gridspec_kw={"height_ratios": [3, 1]}\n)\nfig.suptitle("Synthetic Diff in Diff Estimation")\n\nax1.plot(\n    pivot_df_control.mean(axis=1), lw=3, color="C1", ls="dashed", label="Control Avg."\n)\nax1.plot(treated_mean, lw=3, color="C0", label="California")\nax1.plot(\n    pivot_df_control.index,\n    sc_did,\n    label="Synthetic Control (SDID)",\n    color="C1",\n    alpha=0.8,\n)\nax1.set_ylabel("Cigarette Sales")\nax1.vlines(\n    1989,\n    treated_mean.min(),\n    treated_mean.max(),\n    color="black",\n    ls="dotted",\n    label="Prop. 99",\n)\nax1.legend()\n\nax2.bar(time_weights.index, time_weights["value"], color="skyblue")\nax2.set_ylabel("Time Weights")\nax2.set_xlabel("Time")\nax2.vlines(1989, 0, 1, color="black", ls="dotted")\n')))}p.isMDXComponent=!0}}]);