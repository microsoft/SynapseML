"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[85729],{3905:(e,t,n)=>{n.d(t,{Zo:()=>m,kt:()=>d});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},l=Object.keys(e);for(a=0;a<l.length;a++)n=l[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)n=l[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),u=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},m=function(e){var t=u(e.components);return a.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},p=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,l=e.originalType,s=e.parentName,m=o(e,["components","mdxType","originalType","parentName"]),p=u(n),d=r,f=p["".concat(s,".").concat(d)]||p[d]||c[d]||l;return n?a.createElement(f,i(i({ref:t},m),{},{components:n})):a.createElement(f,i({ref:t},m))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=n.length,i=new Array(l);i[0]=p;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:r,i[1]=o;for(var u=2;u<l;u++)i[u]=n[u];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}p.displayName="MDXCreateElement"},85162:(e,t,n)=>{n.d(t,{Z:()=>i});var a=n(67294),r=n(86010);const l="tabItem_Ymn6";function i(e){let{children:t,hidden:n,className:i}=e;return a.createElement("div",{role:"tabpanel",className:(0,r.Z)(l,i),hidden:n},t)}},74866:(e,t,n)=>{n.d(t,{Z:()=>T});var a=n(83117),r=n(67294),l=n(86010),i=n(12466),o=n(16550),s=n(91980),u=n(67392),m=n(50012);function c(e){return function(e){var t;return(null==(t=r.Children.map(e,(e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})))?void 0:t.filter(Boolean))??[]}(e).map((e=>{let{props:{value:t,label:n,attributes:a,default:r}}=e;return{value:t,label:n,attributes:a,default:r}}))}function p(e){const{values:t,children:n}=e;return(0,r.useMemo)((()=>{const e=t??c(n);return function(e){const t=(0,u.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,n])}function d(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function f(e){let{queryString:t=!1,groupId:n}=e;const a=(0,o.k6)(),l=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return n??null}({queryString:t,groupId:n});return[(0,s._X)(l),(0,r.useCallback)((e=>{if(!l)return;const t=new URLSearchParams(a.location.search);t.set(l,e),a.replace({...a.location,search:t.toString()})}),[l,a])]}function v(e){const{defaultValue:t,queryString:n=!1,groupId:a}=e,l=p(e),[i,o]=(0,r.useState)((()=>function(e){let{defaultValue:t,tabValues:n}=e;if(0===n.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!d({value:t,tabValues:n}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${n.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const a=n.find((e=>e.default))??n[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:t,tabValues:l}))),[s,u]=f({queryString:n,groupId:a}),[c,v]=function(e){let{groupId:t}=e;const n=function(e){return e?`docusaurus.tab.${e}`:null}(t),[a,l]=(0,m.Nk)(n);return[a,(0,r.useCallback)((e=>{n&&l.set(e)}),[n,l])]}({groupId:a}),y=(()=>{const e=s??c;return d({value:e,tabValues:l})?e:null})();(0,r.useLayoutEffect)((()=>{y&&o(y)}),[y]);return{selectedValue:i,selectValue:(0,r.useCallback)((e=>{if(!d({value:e,tabValues:l}))throw new Error(`Can't select invalid tab value=${e}`);o(e),u(e),v(e)}),[u,v,l]),tabValues:l}}var y=n(72389);const b="tabList__CuJ",g="tabItem_LNqP";function h(e){let{className:t,block:n,selectedValue:o,selectValue:s,tabValues:u}=e;const m=[],{blockElementScrollPositionUntilNextRender:c}=(0,i.o5)(),p=e=>{const t=e.currentTarget,n=m.indexOf(t),a=u[n].value;a!==o&&(c(t),s(a))},d=e=>{var t;let n=null;switch(e.key){case"Enter":p(e);break;case"ArrowRight":{const t=m.indexOf(e.currentTarget)+1;n=m[t]??m[0];break}case"ArrowLeft":{const t=m.indexOf(e.currentTarget)-1;n=m[t]??m[m.length-1];break}}null==(t=n)||t.focus()};return r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":n},t)},u.map((e=>{let{value:t,label:n,attributes:i}=e;return r.createElement("li",(0,a.Z)({role:"tab",tabIndex:o===t?0:-1,"aria-selected":o===t,key:t,ref:e=>m.push(e),onKeyDown:d,onClick:p},i,{className:(0,l.Z)("tabs__item",g,null==i?void 0:i.className,{"tabs__item--active":o===t})}),n??t)})))}function E(e){let{lazy:t,children:n,selectedValue:a}=e;const l=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=l.find((e=>e.props.value===a));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return r.createElement("div",{className:"margin-top--md"},l.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==a}))))}function w(e){const t=v(e);return r.createElement("div",{className:(0,l.Z)("tabs-container",b)},r.createElement(h,(0,a.Z)({},e,t)),r.createElement(E,(0,a.Z)({},e,t)))}function T(e){const t=(0,y.Z)();return r.createElement(w,(0,a.Z)({key:String(t)},e))}},31989:(e,t,n)=>{n.d(t,{Z:()=>l});var a=n(67294),r=n(52263);const l=function(e){const{className:t,py:n,scala:l,csharp:i,sourceLink:o}=e,s=(0,r.Z)().siteConfig.customFields.version;let u=`https://mmlspark.blob.core.windows.net/docs/${s}/pyspark/${n}`,m=`https://mmlspark.blob.core.windows.net/docs/${s}/scala/${l}`;return a.createElement("table",null,a.createElement("tbody",null,a.createElement("tr",null,a.createElement("td",null,a.createElement("strong",null,"Python API: "),a.createElement("a",{href:u},t)),a.createElement("td",null,a.createElement("strong",null,"Scala API: "),a.createElement("a",{href:m},t)),a.createElement("td",null,a.createElement("strong",null,"Source: "),a.createElement("a",{href:o},t)))))}},48482:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>f,contentTitle:()=>p,default:()=>b,frontMatter:()=>c,metadata:()=>d,toc:()=>v});var a=n(83117),r=(n(67294),n(3905)),l=n(74866),i=n(85162),o=n(31989);const s=[{value:"SimpleFitMultivariateAnomaly",id:"simplefitmultivariateanomaly",level:2}],u={toc:s};function m(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"simplefitmultivariateanomaly"},"SimpleFitMultivariateAnomaly"),(0,r.kt)(l.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,r.kt)(i.Z,{value:"py",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services import *\n\nanomalyKey = os.environ.get("ANOMALY_API_KEY", getSecret("anomaly-api-key"))\nstartTime = "2021-01-01T00:00:00Z"\nendTime = "2021-01-03T01:59:00Z"\ntimestampColumn = "timestamp"\ninputColumns = ["feature0", "feature1", "feature2"]\nintermediateSaveDir = "wasbs://madtest@anomalydetectiontest.blob.core.windows.net/intermediateData"\n\nsimpleFitMultivariateAnomaly = (SimpleFitMultivariateAnomaly()\n    .setSubscriptionKey(anomalyKey)\n    .setLocation("westus2")\n    .setOutputCol("result")\n    .setStartTime(startTime)\n    .setEndTime(endTime)\n    .setIntermediateSaveDir(intermediateSaveDir)\n    .setTimestampCol(timestampColumn)\n    .setInputCols(inputColumns)\n    .setSlidingWindow(50))\n\n# uncomment below for fitting your own dataframe\n# model = simpleFitMultivariateAnomaly.fit(df)\n# simpleFitMultivariateAnomaly.cleanUpIntermediateData()\n'))),(0,r.kt)(i.Z,{value:"scala",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.services.anomaly.FitMultivariateAnomaly\n\nval startTime: String = "2021-01-01T00:00:00Z"\nval endTime: String = "2021-01-02T12:00:00Z"\nval timestampColumn: String = "timestamp"\nval inputColumns: Array[String] = Array("feature0", "feature1", "feature2")\nval intermediateSaveDir: String = "wasbs://madtest@anomalydetectiontest.blob.core.windows.net/intermediateData"\nval anomalyKey = sys.env.getOrElse("ANOMALY_API_KEY", None)\n\nval simpleFitMultivariateAnomaly = (new SimpleFitMultivariateAnomaly()\n  .setSubscriptionKey(anomalyKey)\n  .setLocation("westus2")\n  .setOutputCol("result")\n  .setStartTime(startTime)\n  .setEndTime(endTime)\n  .setIntermediateSaveDir(intermediateSaveDir)\n  .setTimestampCol(timestampColumn)\n  .setInputCols(inputColumns)\n  .setSlidingWindow(50))\n\nval df = (spark.read.format("csv")\n  .option("header", True)\n  .load("wasbs://datasets@mmlspark.blob.core.windows.net/MAD/mad_example.csv"))\n\nval model = simpleFitMultivariateAnomaly.fit(df)\n\nval result = (model\n  .setStartTime(startTime)\n  .setEndTime(endTime)\n  .setOutputCol("result")\n  .setTimestampCol(timestampColumn)\n  .setInputCols(inputColumns)\n  .transform(df))\n\nresult.show()\n\nsimpleFitMultivariateAnomaly.cleanUpIntermediateData()\nmodel.cleanUpIntermediateData()\n')))),(0,r.kt)(o.Z,{className:"SimpleFitMultivariateAnomaly",py:"synapse.ml.cognitive.html#module-synapse.ml.cognitive.SimpleFitMultivariateAnomaly",scala:"com/microsoft/azure/synapse/ml/cognitive/SimpleFitMultivariateAnomaly.html",csharp:"classSynapse_1_1ML_1_1Cognitive_1_1SimpleFitMultivariateAnomaly.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/MultivariateAnomalyDetection.scala",mdxType:"DocTable"}))}m.isMDXComponent=!0;const c={title:"Estimators - Cognitive",sidebar_label:"Cognitive",hide_title:!0},p=void 0,d={unversionedId:"Quick Examples/estimators/estimators_cognitive",id:"version-1.0.2/Quick Examples/estimators/estimators_cognitive",title:"Estimators - Cognitive",description:"",source:"@site/versioned_docs/version-1.0.2/Quick Examples/estimators/estimators_cognitive.md",sourceDirName:"Quick Examples/estimators",slug:"/Quick Examples/estimators/estimators_cognitive",permalink:"/SynapseML/docs/1.0.2/Quick Examples/estimators/estimators_cognitive",draft:!1,tags:[],version:"1.0.2",frontMatter:{title:"Estimators - Cognitive",sidebar_label:"Cognitive",hide_title:!0}},f={},v=[...s],y={toc:v};function b(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},y,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)(m,{mdxType:"MAD"}))}b.isMDXComponent=!0}}]);