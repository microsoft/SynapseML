"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[6362],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>d});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function s(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var i=n.createContext({}),u=function(e){var t=n.useContext(i),a=t;return e&&(a="function"==typeof e?e(t):s(s({},t),e)),a},c=function(e){var t=u(e.components);return n.createElement(i.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},p=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,i=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),p=u(a),d=r,f=p["".concat(i,".").concat(d)]||p[d]||m[d]||o;return a?n.createElement(f,s(s({ref:t},c),{},{components:a})):n.createElement(f,s({ref:t},c))}));function d(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,s=new Array(o);s[0]=p;var l={};for(var i in t)hasOwnProperty.call(t,i)&&(l[i]=t[i]);l.originalType=e,l.mdxType="string"==typeof e?e:r,s[1]=l;for(var u=2;u<o;u++)s[u]=a[u];return n.createElement.apply(null,s)}return n.createElement.apply(null,a)}p.displayName="MDXCreateElement"},24052:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>i,contentTitle:()=>s,default:()=>m,frontMatter:()=>o,metadata:()=>l,toc:()=>u});var n=a(83117),r=(a(67294),a(3905));const o={title:"Quickstart - Measure Causal Effects",hide_title:!0,status:"stable"},s="Startup Investment Attribution - Understand Outreach Effort's Effect",l={unversionedId:"Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects",id:"version-1.0.6/Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects",title:"Quickstart - Measure Causal Effects",description:"This sample notebook aims to show the application of using SynapseML's DoubleMLEstimator for inferring causality using observational data.",source:"@site/versioned_docs/version-1.0.6/Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects.md",sourceDirName:"Explore Algorithms/Causal Inference",slug:"/Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects",draft:!1,tags:[],version:"1.0.6",frontMatter:{title:"Quickstart - Measure Causal Effects",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Overview",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Causal Inference/Overview"},next:{title:"Quickstart - Measure Heterogeneous Effects",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Causal Inference/Quickstart - Measure Heterogeneous Effects"}},i={},u=[{value:"Background",id:"background",level:2},{value:"Data",id:"data",level:2},{value:"Get Causal Effects with SynapseML DoubleMLEstimator",id:"get-causal-effects-with-synapseml-doublemlestimator",level:2}],c={toc:u};function m(e){let{components:t,...a}=e;return(0,r.kt)("wrapper",(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"startup-investment-attribution---understand-outreach-efforts-effect"},"Startup Investment Attribution - Understand Outreach Effort's Effect"),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"This sample notebook aims to show the application of using SynapseML's DoubleMLEstimator for inferring causality using observational data.")),(0,r.kt)("p",null,"A startup that sells software would like to know whether its outreach efforts were successful in attracting new customers or boosting consumption among existing customers. In other words, they would like to learn the treatment effect of each investment on customers' software usage."),(0,r.kt)("p",null,"In an ideal world, the startup would run several randomized experiments where each customer would receive a random assortment of investments. However, this can be logistically prohibitive or strategically unsound: the startup might not have the resources to design such experiments or they might not want to risk losing out on big opportunities due to lack of incentives."),(0,r.kt)("p",null,"In this customer scenario walkthrough, we show how SynapseML causal package can use historical investment data to learn the investment effect."),(0,r.kt)("h2",{id:"background"},"Background"),(0,r.kt)("p",null,"In this scenario, a startup that sells software provides discounts incentives to its customer. A customer might be given or not."),(0,r.kt)("p",null,"The startup has historical data on these investments for 2,000 customers, as well as how much revenue these customers generated in the year after the investments were made. They would like to use this data to learn the optimal incentive policy for each existing or new customer in order to maximize the return on investment (ROI)."),(0,r.kt)("p",null,"The startup faces a challenge:  the dataset is biased because historically the larger customers received the most incentives. Thus, they need a causal model that can remove the bias."),(0,r.kt)("h2",{id:"data"},"Data"),(0,r.kt)("p",null,"The data* contains ~2,000 customers and is comprised of:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Customer features: details about the industry, size, revenue, and technology profile of each customer."),(0,r.kt)("li",{parentName:"ul"},"Interventions: information about which incentive was given to a customer."),(0,r.kt)("li",{parentName:"ul"},"Outcome: the amount of product the customer bought in the year after the incentives were given.")),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Feature Name"),(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"Details"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Global Flag"),(0,r.kt)("td",{parentName:"tr",align:null},"W"),(0,r.kt)("td",{parentName:"tr",align:null},"whether the customer has global offices")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Major Flag"),(0,r.kt)("td",{parentName:"tr",align:null},"W"),(0,r.kt)("td",{parentName:"tr",align:null},"whether the customer is a large consumer in their industry (as opposed to SMC - Small Medium Corporation - or SMB - Small Medium Business)")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"SMC Flag"),(0,r.kt)("td",{parentName:"tr",align:null},"W"),(0,r.kt)("td",{parentName:"tr",align:null},"whether the customer is a Small Medium Corporation (SMC, as opposed to major and SMB)")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Commercial Flag"),(0,r.kt)("td",{parentName:"tr",align:null},"W"),(0,r.kt)("td",{parentName:"tr",align:null},"whether the customer's business is commercial (as opposed to public secor)")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"IT Spend"),(0,r.kt)("td",{parentName:"tr",align:null},"W"),(0,r.kt)("td",{parentName:"tr",align:null},"$ spent on IT-related purchases")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Employee Count"),(0,r.kt)("td",{parentName:"tr",align:null},"W"),(0,r.kt)("td",{parentName:"tr",align:null},"number of employees")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"PC Count"),(0,r.kt)("td",{parentName:"tr",align:null},"W"),(0,r.kt)("td",{parentName:"tr",align:null},"number of PCs used by the customer")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Discount"),(0,r.kt)("td",{parentName:"tr",align:null},"T"),(0,r.kt)("td",{parentName:"tr",align:null},"whether the customer was given a discount (binary)")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Revenue"),(0,r.kt)("td",{parentName:"tr",align:null},"Y"),(0,r.kt)("td",{parentName:"tr",align:null},"$ Revenue from customer given by the amount of software purchased")))),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'# Import the sample multi-attribution data\ndata = (\n    spark.read.format("csv")\n    .option("inferSchema", True)\n    .option("header", True)\n    .load(\n        "wasbs://publicwasb@mmlspark.blob.core.windows.net/multi_attribution_sample.csv"\n    )\n)\n')),(0,r.kt)("h2",{id:"get-causal-effects-with-synapseml-doublemlestimator"},"Get Causal Effects with SynapseML DoubleMLEstimator"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.causal import *\nfrom pyspark.ml.classification import LogisticRegression\nfrom pyspark.ml.regression import LinearRegression\n\ntreatmentColumn = "Discount"\noutcomeColumn = "Revenue"\n\ndml = (\n    DoubleMLEstimator()\n    .setTreatmentModel(LogisticRegression())\n    .setTreatmentCol(treatmentColumn)\n    .setOutcomeModel(LinearRegression())\n    .setOutcomeCol(outcomeColumn)\n    .setMaxIter(20)\n)\n\nmodel = dml.fit(data)\n')),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"# Get average treatment effect, it returns a numeric value, e.g. 5166.78324\n# It means, on average, customers who received a discount spent $5,166 more on software\nmodel.getAvgTreatmentEffect()\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"# Get treatment effect's confidence interval, e.g.  [4765.826181160708, 5371.2817538168965]\nmodel.getConfidenceInterval()\n")))}m.isMDXComponent=!0}}]);