"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[50783],{3905:(e,t,n)=>{n.d(t,{Zo:()=>i,kt:()=>d});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},l=Object.keys(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var u=r.createContext({}),c=function(e){var t=r.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},i=function(e){var t=c(e.components);return r.createElement(u.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,l=e.originalType,u=e.parentName,i=s(e,["components","mdxType","originalType","parentName"]),m=c(n),d=a,f=m["".concat(u,".").concat(d)]||m[d]||p[d]||l;return n?r.createElement(f,o(o({ref:t},i),{},{components:n})):r.createElement(f,o({ref:t},i))}));function d(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var l=n.length,o=new Array(l);o[0]=m;var s={};for(var u in t)hasOwnProperty.call(t,u)&&(s[u]=t[u]);s.originalType=e,s.mdxType="string"==typeof e?e:a,o[1]=s;for(var c=2;c<l;c++)o[c]=n[c];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},85162:(e,t,n)=>{n.d(t,{Z:()=>o});var r=n(67294),a=n(86010);const l="tabItem_Ymn6";function o(e){let{children:t,hidden:n,className:o}=e;return r.createElement("div",{role:"tabpanel",className:(0,a.Z)(l,o),hidden:n},t)}},74866:(e,t,n)=>{n.d(t,{Z:()=>_});var r=n(83117),a=n(67294),l=n(86010),o=n(12466),s=n(16550),u=n(91980),c=n(67392),i=n(50012);function p(e){return function(e){var t,n;return null!=(t=null==(n=a.Children.map(e,(e=>{if(!e||(0,a.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:n.filter(Boolean))?t:[]}(e).map((e=>{let{props:{value:t,label:n,attributes:r,default:a}}=e;return{value:t,label:n,attributes:r,default:a}}))}function m(e){const{values:t,children:n}=e;return(0,a.useMemo)((()=>{const e=null!=t?t:p(n);return function(e){const t=(0,c.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error('Docusaurus error: Duplicate values "'+t.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[t,n])}function d(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function f(e){let{queryString:t=!1,groupId:n}=e;const r=(0,s.k6)(),l=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=n?n:null}({queryString:t,groupId:n});return[(0,u._X)(l),(0,a.useCallback)((e=>{if(!l)return;const t=new URLSearchParams(r.location.search);t.set(l,e),r.replace({...r.location,search:t.toString()})}),[l,r])]}function b(e){const{defaultValue:t,queryString:n=!1,groupId:r}=e,l=m(e),[o,s]=(0,a.useState)((()=>function(e){var t;let{defaultValue:n,tabValues:r}=e;if(0===r.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(n){if(!d({value:n,tabValues:r}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+n+'" but none of its children has the corresponding value. Available values are: '+r.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return n}const a=null!=(t=r.find((e=>e.default)))?t:r[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:t,tabValues:l}))),[u,c]=f({queryString:n,groupId:r}),[p,b]=function(e){let{groupId:t}=e;const n=function(e){return e?"docusaurus.tab."+e:null}(t),[r,l]=(0,i.Nk)(n);return[r,(0,a.useCallback)((e=>{n&&l.set(e)}),[n,l])]}({groupId:r}),y=(()=>{const e=null!=u?u:p;return d({value:e,tabValues:l})?e:null})();(0,a.useLayoutEffect)((()=>{y&&s(y)}),[y]);return{selectedValue:o,selectValue:(0,a.useCallback)((e=>{if(!d({value:e,tabValues:l}))throw new Error("Can't select invalid tab value="+e);s(e),c(e),b(e)}),[c,b,l]),tabValues:l}}var y=n(72389);const v="tabList__CuJ",h="tabItem_LNqP";function g(e){let{className:t,block:n,selectedValue:s,selectValue:u,tabValues:c}=e;const i=[],{blockElementScrollPositionUntilNextRender:p}=(0,o.o5)(),m=e=>{const t=e.currentTarget,n=i.indexOf(t),r=c[n].value;r!==s&&(p(t),u(r))},d=e=>{var t;let n=null;switch(e.key){case"Enter":m(e);break;case"ArrowRight":{var r;const t=i.indexOf(e.currentTarget)+1;n=null!=(r=i[t])?r:i[0];break}case"ArrowLeft":{var a;const t=i.indexOf(e.currentTarget)-1;n=null!=(a=i[t])?a:i[i.length-1];break}}null==(t=n)||t.focus()};return a.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":n},t)},c.map((e=>{let{value:t,label:n,attributes:o}=e;return a.createElement("li",(0,r.Z)({role:"tab",tabIndex:s===t?0:-1,"aria-selected":s===t,key:t,ref:e=>i.push(e),onKeyDown:d,onClick:m},o,{className:(0,l.Z)("tabs__item",h,null==o?void 0:o.className,{"tabs__item--active":s===t})}),null!=n?n:t)})))}function E(e){let{lazy:t,children:n,selectedValue:r}=e;const l=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=l.find((e=>e.props.value===r));return e?(0,a.cloneElement)(e,{className:"margin-top--md"}):null}return a.createElement("div",{className:"margin-top--md"},l.map(((e,t)=>(0,a.cloneElement)(e,{key:t,hidden:e.props.value!==r}))))}function k(e){const t=b(e);return a.createElement("div",{className:(0,l.Z)("tabs-container",v)},a.createElement(g,(0,r.Z)({},e,t)),a.createElement(E,(0,r.Z)({},e,t)))}function _(e){const t=(0,y.Z)();return a.createElement(k,(0,r.Z)({key:String(t)},e))}},31989:(e,t,n)=>{n.d(t,{Z:()=>l});var r=n(67294),a=n(52263);const l=function(e){const{className:t,py:n,scala:l,csharp:o,sourceLink:s}=e,u=(0,a.Z)().siteConfig.customFields.version;let c="https://mmlspark.blob.core.windows.net/docs/"+u+"/pyspark/"+n,i="https://mmlspark.blob.core.windows.net/docs/"+u+"/scala/"+l,p="https://mmlspark.blob.core.windows.net/docs/"+u+"/dotnet/"+o;return r.createElement("table",null,r.createElement("tbody",null,r.createElement("tr",null,r.createElement("td",null,r.createElement("strong",null,"Python API: "),r.createElement("a",{href:c},t)),r.createElement("td",null,r.createElement("strong",null,"Scala API: "),r.createElement("a",{href:i},t)),r.createElement("td",null,r.createElement("strong",null,".NET API: "),r.createElement("a",{href:p},t)),r.createElement("td",null,r.createElement("strong",null,"Source: "),r.createElement("a",{href:s},t)))))}},2885:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>f,contentTitle:()=>m,default:()=>v,frontMatter:()=>p,metadata:()=>d,toc:()=>b});var r=n(83117),a=(n(67294),n(3905)),l=n(74866),o=n(85162),s=n(31989);const u=[{value:"ONNXModel",id:"onnxmodel",level:2}],c={toc:u};function i(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,r.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"onnxmodel"},"ONNXModel"),(0,a.kt)(l.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,a.kt)(o.Z,{value:"py",mdxType:"TabItem"},(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-py"},'from synapse.ml.onnx import ONNXModel\n\nmodel_path = "PUT_YOUR_MODEL_PATH"\nonnx_ml = (ONNXModel()\n            .setModelLocation(model_path)\n            .setFeedDict({"float_input": "features"})\n            .setFetchDict({"prediction": "output_label", "rawProbability": "output_probability"}))\n'))),(0,a.kt)(o.Z,{value:"scala",mdxType:"TabItem"},(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.onnx._\n\nval model_path = "PUT_YOUR_MODEL_PATH"\nval onnx_ml = (new ONNXModel()\n                  .setModelLocation(model_path)\n                  .setFeedDict(Map("float_input" -> "features"))\n                  .setFetchDict(Map("prediction" -> "output_label", "rawProbability" -> "output_probability")))\n')))),(0,a.kt)(s.Z,{className:"ONNXModel",py:"synapse.ml.onnx.html#module-synapse.ml.onnx.ONNXModel",scala:"com/microsoft/azure/synapse/ml/onnx/ONNXModel.html",csharp:"classSynapse_1_1ML_1_1Onnx_1_1ONNXModel.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/deep-learning/src/main/scala/com/microsoft/azure/synapse/ml/onnx/ONNXModel.scala",mdxType:"DocTable"}))}i.isMDXComponent=!0;const p={title:"Deep Learning",sidebar_label:"Deep Learning"},m=void 0,d={unversionedId:"Quick Examples/transformers/transformers_deep_learning",id:"version-1.0.4/Quick Examples/transformers/transformers_deep_learning",title:"Deep Learning",description:"",source:"@site/versioned_docs/version-1.0.4/Quick Examples/transformers/transformers_deep_learning.md",sourceDirName:"Quick Examples/transformers",slug:"/Quick Examples/transformers/transformers_deep_learning",permalink:"/SynapseML/docs/Quick Examples/transformers/transformers_deep_learning",draft:!1,tags:[],version:"1.0.4",frontMatter:{title:"Deep Learning",sidebar_label:"Deep Learning"}},f={},b=[...u],y={toc:b};function v(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,r.Z)({},y,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)(i,{mdxType:"ONNXModel"}))}v.isMDXComponent=!0}}]);