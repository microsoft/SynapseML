"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[47333],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>d});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),c=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},u=function(e){var t=c(e.components);return a.createElement(s.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),m=c(n),d=r,f=m["".concat(s,".").concat(d)]||m[d]||p[d]||o;return n?a.createElement(f,l(l({ref:t},u),{},{components:n})):a.createElement(f,l({ref:t},u))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,l=new Array(o);l[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:r,l[1]=i;for(var c=2;c<o;c++)l[c]=n[c];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},72076:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>p,frontMatter:()=>o,metadata:()=>i,toc:()=>c});var a=n(83117),r=(n(67294),n(3905));const o={title:"Overview",hide_title:!0,sidebar_label:"Overview"},l=void 0,i={unversionedId:"Explore Algorithms/Causal Inference/Overview",id:"version-1.0.9/Explore Algorithms/Causal Inference/Overview",title:"Overview",description:"Causal Inference on Apache Spark",source:"@site/versioned_docs/version-1.0.9/Explore Algorithms/Causal Inference/Overview.md",sourceDirName:"Explore Algorithms/Causal Inference",slug:"/Explore Algorithms/Causal Inference/Overview",permalink:"/SynapseML/docs/1.0.9/Explore Algorithms/Causal Inference/Overview",draft:!1,tags:[],version:"1.0.9",frontMatter:{title:"Overview",hide_title:!0,sidebar_label:"Overview"},sidebar:"docs",previous:{title:"Quickstart - Snow Leopard Detection",permalink:"/SynapseML/docs/1.0.9/Explore Algorithms/Responsible AI/Quickstart - Snow Leopard Detection"},next:{title:"Quickstart - Measure Causal Effects",permalink:"/SynapseML/docs/1.0.9/Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects"}},s={},c=[{value:"Causal Inference on Apache Spark",id:"causal-inference-on-apache-spark",level:2},{value:"What is Causal Inference?",id:"what-is-causal-inference",level:3},{value:"Causal Inference language",id:"causal-inference-language",level:3},{value:"Causal Inference and Double machine learning",id:"causal-inference-and-double-machine-learning",level:3},{value:"Usage",id:"usage",level:3}],u={toc:c};function p(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"causal-inference-on-apache-spark"},"Causal Inference on Apache Spark"),(0,r.kt)("h3",{id:"what-is-causal-inference"},"What is Causal Inference?"),(0,r.kt)("p",null,"One challenge that has taken the spotlight in recent years is using machine learning to drive decision makings in policy and business.\nOften, businesses and policymakers would like to study whether an incentive or intervention will lead to a desired outcome and by how much.\nFor example, if we give customers a discount (treatment), how much more will they purchase in the future (outcome).\nTraditionally, people use correlation analysis or prediction model to understand correlated factors, but going from prediction to an\nimpactful decision isn't always straightforward as correlation doesn't imply causation. In many cases, confounding variables influence\nboth the probability of treatment and the outcome, introducing more non-causal correlation. "),(0,r.kt)("p",null,"Causal inference helps to bridge the gap between prediction and decision-making. "),(0,r.kt)("h3",{id:"causal-inference-language"},"Causal Inference language"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Term"),(0,r.kt)("th",{parentName:"tr",align:null},"Example"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Treatment (T)"),(0,r.kt)("td",{parentName:"tr",align:null},"Seeing an advertisement")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Outcome (Y)"),(0,r.kt)("td",{parentName:"tr",align:null},"Probability of buying a specific new game")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Confounders (W)"),(0,r.kt)("td",{parentName:"tr",align:null},"Current gaming habits, past purchases, customer location, platform")))),(0,r.kt)("h3",{id:"causal-inference-and-double-machine-learning"},"Causal Inference and Double machine learning"),(0,r.kt)("p",null,"The gold standard approach to isolating causal questions is to run an experiment that randomly assigns the treatment to some customers.\nRandomization eliminates any relationship between the confounders and the probability of treatment,\nso any differences between treated and untreated customers can only reflect the direct causal effect of the treatment on the outcome (treatment effect).\nHowever, in many cases, treatments experiments are either impossible or cost prohibitive.\nAs a result, we look toward causal inference methods that allow us to estimate the treatment effect using observational data."),(0,r.kt)("p",null,'The SynapseML causal package implements a technique "Double machine learning", which can be used to estimate the average treatment effect via machine learning models.\nUnlike regression-based approaches that make strict parametric assumptions, this machine learning-based approach allows us to model non-linear      relationships between the confounders, treatment, and outcome.'),(0,r.kt)("h3",{id:"usage"},"Usage"),(0,r.kt)("p",null,"In PySpark, you can run the ",(0,r.kt)("inlineCode",{parentName:"p"},"DoubleMLEstimator")," via:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.ml.classification import LogisticRegression\nfrom synapse.ml.causal import DoubleMLEstimator\ndml = (DoubleMLEstimator()\n      .setTreatmentCol("Treatment")\n      .setTreatmentModel(LogisticRegression())\n      .setOutcomeCol("Outcome")\n      .setOutcomeModel(LogisticRegression())\n      .setMaxIter(20))\ndmlModel = dml.fit(dataset)\n')),(0,r.kt)("blockquote",null,(0,r.kt)("p",{parentName:"blockquote"},'Note: all columns except "Treatment" and "Outcome" in your dataset will be used as confounders.')),(0,r.kt)("blockquote",null,(0,r.kt)("p",{parentName:"blockquote"},"Note: For discrete treatment, the treatment column must be ",(0,r.kt)("inlineCode",{parentName:"p"},"int")," or ",(0,r.kt)("inlineCode",{parentName:"p"},"bool"),". ",(0,r.kt)("inlineCode",{parentName:"p"},"0")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"False")," will be treated as the control group. ")),(0,r.kt)("p",null,"After fitting the model, you can get average treatment effect and confidence interval:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"dmlModel.getAvgTreatmentEffect()\ndmlModel.getConfidenceInterval()\n")),(0,r.kt)("p",null,"For an end to end application, check out the DoubleMLEstimator ",(0,r.kt)("a",{parentName:"p",href:"../Quickstart%20-%20Measure%20Causal%20Effects"},"notebook\nexample"),"."))}p.isMDXComponent=!0}}]);