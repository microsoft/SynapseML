"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[83393],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>b});var r=a(67294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},l=Object.keys(e);for(r=0;r<l.length;r++)a=l[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)a=l[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var u=r.createContext({}),i=function(e){var t=r.useContext(u),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},c=function(e){var t=i(e.components);return r.createElement(u.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,l=e.originalType,u=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),m=i(a),b=n,d=m["".concat(u,".").concat(b)]||m[b]||p[b]||l;return a?r.createElement(d,o(o({ref:t},c),{},{components:a})):r.createElement(d,o({ref:t},c))}));function b(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var l=a.length,o=new Array(l);o[0]=m;var s={};for(var u in t)hasOwnProperty.call(t,u)&&(s[u]=t[u]);s.originalType=e,s.mdxType="string"==typeof e?e:n,o[1]=s;for(var i=2;i<l;i++)o[i]=a[i];return r.createElement.apply(null,o)}return r.createElement.apply(null,a)}m.displayName="MDXCreateElement"},85162:(e,t,a)=>{a.d(t,{Z:()=>o});var r=a(67294),n=a(86010);const l="tabItem_Ymn6";function o(e){let{children:t,hidden:a,className:o}=e;return r.createElement("div",{role:"tabpanel",className:(0,n.Z)(l,o),hidden:a},t)}},74866:(e,t,a)=>{a.d(t,{Z:()=>E});var r=a(83117),n=a(67294),l=a(86010),o=a(12466),s=a(16550),u=a(91980),i=a(67392),c=a(50012);function p(e){return function(e){var t;return(null==(t=n.Children.map(e,(e=>{if(!e||(0,n.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})))?void 0:t.filter(Boolean))??[]}(e).map((e=>{let{props:{value:t,label:a,attributes:r,default:n}}=e;return{value:t,label:a,attributes:r,default:n}}))}function m(e){const{values:t,children:a}=e;return(0,n.useMemo)((()=>{const e=t??p(a);return function(e){const t=(0,i.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,a])}function b(e){let{value:t,tabValues:a}=e;return a.some((e=>e.value===t))}function d(e){let{queryString:t=!1,groupId:a}=e;const r=(0,s.k6)(),l=function(e){let{queryString:t=!1,groupId:a}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!a)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return a??null}({queryString:t,groupId:a});return[(0,u._X)(l),(0,n.useCallback)((e=>{if(!l)return;const t=new URLSearchParams(r.location.search);t.set(l,e),r.replace({...r.location,search:t.toString()})}),[l,r])]}function f(e){const{defaultValue:t,queryString:a=!1,groupId:r}=e,l=m(e),[o,s]=(0,n.useState)((()=>function(e){let{defaultValue:t,tabValues:a}=e;if(0===a.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!b({value:t,tabValues:a}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${a.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const r=a.find((e=>e.default))??a[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:t,tabValues:l}))),[u,i]=d({queryString:a,groupId:r}),[p,f]=function(e){let{groupId:t}=e;const a=function(e){return e?`docusaurus.tab.${e}`:null}(t),[r,l]=(0,c.Nk)(a);return[r,(0,n.useCallback)((e=>{a&&l.set(e)}),[a,l])]}({groupId:r}),v=(()=>{const e=u??p;return b({value:e,tabValues:l})?e:null})();(0,n.useLayoutEffect)((()=>{v&&s(v)}),[v]);return{selectedValue:o,selectValue:(0,n.useCallback)((e=>{if(!b({value:e,tabValues:l}))throw new Error(`Can't select invalid tab value=${e}`);s(e),i(e),f(e)}),[i,f,l]),tabValues:l}}var v=a(72389);const y="tabList__CuJ",w="tabItem_LNqP";function h(e){let{className:t,block:a,selectedValue:s,selectValue:u,tabValues:i}=e;const c=[],{blockElementScrollPositionUntilNextRender:p}=(0,o.o5)(),m=e=>{const t=e.currentTarget,a=c.indexOf(t),r=i[a].value;r!==s&&(p(t),u(r))},b=e=>{var t;let a=null;switch(e.key){case"Enter":m(e);break;case"ArrowRight":{const t=c.indexOf(e.currentTarget)+1;a=c[t]??c[0];break}case"ArrowLeft":{const t=c.indexOf(e.currentTarget)-1;a=c[t]??c[c.length-1];break}}null==(t=a)||t.focus()};return n.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":a},t)},i.map((e=>{let{value:t,label:a,attributes:o}=e;return n.createElement("li",(0,r.Z)({role:"tab",tabIndex:s===t?0:-1,"aria-selected":s===t,key:t,ref:e=>c.push(e),onKeyDown:b,onClick:m},o,{className:(0,l.Z)("tabs__item",w,null==o?void 0:o.className,{"tabs__item--active":s===t})}),a??t)})))}function g(e){let{lazy:t,children:a,selectedValue:r}=e;const l=(Array.isArray(a)?a:[a]).filter(Boolean);if(t){const e=l.find((e=>e.props.value===r));return e?(0,n.cloneElement)(e,{className:"margin-top--md"}):null}return n.createElement("div",{className:"margin-top--md"},l.map(((e,t)=>(0,n.cloneElement)(e,{key:t,hidden:e.props.value!==r}))))}function k(e){const t=f(e);return n.createElement("div",{className:(0,l.Z)("tabs-container",y)},n.createElement(h,(0,r.Z)({},e,t)),n.createElement(g,(0,r.Z)({},e,t)))}function E(e){const t=(0,v.Z)();return n.createElement(k,(0,r.Z)({key:String(t)},e))}},31989:(e,t,a)=>{a.d(t,{Z:()=>l});var r=a(67294),n=a(52263);const l=function(e){const{className:t,py:a,scala:l,csharp:o,sourceLink:s}=e,u=(0,n.Z)().siteConfig.customFields.version;let i=`https://mmlspark.blob.core.windows.net/docs/${u}/pyspark/${a}`,c=`https://mmlspark.blob.core.windows.net/docs/${u}/scala/${l}`;return r.createElement("table",null,r.createElement("tbody",null,r.createElement("tr",null,r.createElement("td",null,r.createElement("strong",null,"Python API: "),r.createElement("a",{href:i},t)),r.createElement("td",null,r.createElement("strong",null,"Scala API: "),r.createElement("a",{href:c},t)),r.createElement("td",null,r.createElement("strong",null,"Source: "),r.createElement("a",{href:s},t)))))}},73297:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>d,contentTitle:()=>m,default:()=>y,frontMatter:()=>p,metadata:()=>b,toc:()=>f});var r=a(83117),n=(a(67294),a(3905)),l=a(74866),o=a(85162),s=a(31989);const u=[{value:"VowpalWabbitRegressor",id:"vowpalwabbitregressor",level:2},{value:"VowpalWabbitContextualBandit",id:"vowpalwabbitcontextualbandit",level:2}],i={toc:u};function c(e){let{components:t,...a}=e;return(0,n.kt)("wrapper",(0,r.Z)({},i,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h2",{id:"vowpalwabbitregressor"},"VowpalWabbitRegressor"),(0,n.kt)(l.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,n.kt)(o.Z,{value:"py",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import *\n\nvw = (VowpalWabbitRegressor()\n      .setLabelCol("Y1")\n      .setFeaturesCol("features")\n      .setPredictionCol("pred"))\n\nvwRegressor = (VowpalWabbitRegressor()\n      .setNumPasses(20)\n      .setPassThroughArgs("--holdout_off --loss_function quantile -q :: -l 0.1"))\n'))),(0,n.kt)(o.Z,{value:"scala",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.vw._\n\nval vw = (new VowpalWabbitRegressor()\n  .setLabelCol("Y1")\n  .setFeaturesCol("features")\n  .setPredictionCol("pred"))\n\nval vwRegressor = (new VowpalWabbitRegressor()\n  .setNumPasses(20)\n  .setPassThroughArgs("--holdout_off --loss_function quantile -q :: -l 0.1"))\n\n')))),(0,n.kt)(s.Z,{className:"VowpalWabbitRegressor",py:"synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitRegressor",scala:"com/microsoft/azure/synapse/ml/vw/VowpalWabbitRegressor.html",csharp:"classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitRegressor.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitRegressor.scala",mdxType:"DocTable"}),(0,n.kt)("h2",{id:"vowpalwabbitcontextualbandit"},"VowpalWabbitContextualBandit"),(0,n.kt)(l.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,n.kt)(o.Z,{value:"py",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import *\n\ncb = (VowpalWabbitContextualBandit()\n      .setPassThroughArgs("--cb_explore_adf --epsilon 0.2 --quiet")\n      .setLabelCol("cost")\n      .setProbabilityCol("prob")\n      .setChosenActionCol("chosen_action")\n      .setSharedCol("shared_features")\n      .setFeaturesCol("action_features")\n      .setUseBarrierExecutionMode(False))\n'))),(0,n.kt)(o.Z,{value:"scala",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.vw._\n\nval cb = (new VowpalWabbitContextualBandit()\n  .setPassThroughArgs("--cb_explore_adf --epsilon 0.2 --quiet")\n  .setLabelCol("cost")\n  .setProbabilityCol("prob")\n  .setChosenActionCol("chosen_action")\n  .setSharedCol("shared_features")\n  .setFeaturesCol("action_features")\n  .setUseBarrierExecutionMode(false))\n\n')))),(0,n.kt)(s.Z,{className:"VowpalWabbitContextualBandit",py:"synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitContextualBandit",scala:"com/microsoft/azure/synapse/ml/vw/VowpalWabbitContextualBandit.html",csharp:"classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitContextualBandit.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitContextualBandit.scala",mdxType:"DocTable"}))}c.isMDXComponent=!0;const p={title:"Estimators - Vowpal Wabbit",sidebar_label:"Vowpal Wabbit",hide_title:!0},m="Vowpal Wabbit",b={unversionedId:"Quick Examples/estimators/estimators_vw",id:"version-1.0.2/Quick Examples/estimators/estimators_vw",title:"Estimators - Vowpal Wabbit",description:"",source:"@site/versioned_docs/version-1.0.2/Quick Examples/estimators/estimators_vw.md",sourceDirName:"Quick Examples/estimators",slug:"/Quick Examples/estimators/estimators_vw",permalink:"/SynapseML/docs/1.0.2/Quick Examples/estimators/estimators_vw",draft:!1,tags:[],version:"1.0.2",frontMatter:{title:"Estimators - Vowpal Wabbit",sidebar_label:"Vowpal Wabbit",hide_title:!0}},d={},f=[...u],v={toc:f};function y(e){let{components:t,...a}=e;return(0,n.kt)("wrapper",(0,r.Z)({},v,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"vowpal-wabbit"},"Vowpal Wabbit"),(0,n.kt)(c,{mdxType:"VW"}))}y.isMDXComponent=!0}}]);