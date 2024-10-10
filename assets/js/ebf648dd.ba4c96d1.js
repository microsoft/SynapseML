"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[15566],{3905:(e,t,a)=>{a.d(t,{Zo:()=>u,kt:()=>b});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function s(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var i=n.createContext({}),c=function(e){var t=n.useContext(i),a=t;return e&&(a="function"==typeof e?e(t):s(s({},t),e)),a},u=function(e){var t=c(e.components);return n.createElement(i.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,i=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),m=c(a),b=r,f=m["".concat(i,".").concat(b)]||m[b]||p[b]||o;return a?n.createElement(f,s(s({ref:t},u),{},{components:a})):n.createElement(f,s({ref:t},u))}));function b(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,s=new Array(o);s[0]=m;var l={};for(var i in t)hasOwnProperty.call(t,i)&&(l[i]=t[i]);l.originalType=e,l.mdxType="string"==typeof e?e:r,s[1]=l;for(var c=2;c<o;c++)s[c]=a[c];return n.createElement.apply(null,s)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},85162:(e,t,a)=>{a.d(t,{Z:()=>s});var n=a(67294),r=a(86010);const o="tabItem_Ymn6";function s(e){let{children:t,hidden:a,className:s}=e;return n.createElement("div",{role:"tabpanel",className:(0,r.Z)(o,s),hidden:a},t)}},74866:(e,t,a)=>{a.d(t,{Z:()=>V});var n=a(83117),r=a(67294),o=a(86010),s=a(12466),l=a(16550),i=a(91980),c=a(67392),u=a(50012);function p(e){return function(e){var t;return(null==(t=r.Children.map(e,(e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})))?void 0:t.filter(Boolean))??[]}(e).map((e=>{let{props:{value:t,label:a,attributes:n,default:r}}=e;return{value:t,label:a,attributes:n,default:r}}))}function m(e){const{values:t,children:a}=e;return(0,r.useMemo)((()=>{const e=t??p(a);return function(e){const t=(0,c.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,a])}function b(e){let{value:t,tabValues:a}=e;return a.some((e=>e.value===t))}function f(e){let{queryString:t=!1,groupId:a}=e;const n=(0,l.k6)(),o=function(e){let{queryString:t=!1,groupId:a}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!a)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return a??null}({queryString:t,groupId:a});return[(0,i._X)(o),(0,r.useCallback)((e=>{if(!o)return;const t=new URLSearchParams(n.location.search);t.set(o,e),n.replace({...n.location,search:t.toString()})}),[o,n])]}function w(e){const{defaultValue:t,queryString:a=!1,groupId:n}=e,o=m(e),[s,l]=(0,r.useState)((()=>function(e){let{defaultValue:t,tabValues:a}=e;if(0===a.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!b({value:t,tabValues:a}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${a.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const n=a.find((e=>e.default))??a[0];if(!n)throw new Error("Unexpected error: 0 tabValues");return n.value}({defaultValue:t,tabValues:o}))),[i,c]=f({queryString:a,groupId:n}),[p,w]=function(e){let{groupId:t}=e;const a=function(e){return e?`docusaurus.tab.${e}`:null}(t),[n,o]=(0,u.Nk)(a);return[n,(0,r.useCallback)((e=>{a&&o.set(e)}),[a,o])]}({groupId:n}),v=(()=>{const e=i??p;return b({value:e,tabValues:o})?e:null})();(0,r.useLayoutEffect)((()=>{v&&l(v)}),[v]);return{selectedValue:s,selectValue:(0,r.useCallback)((e=>{if(!b({value:e,tabValues:o}))throw new Error(`Can't select invalid tab value=${e}`);l(e),c(e),w(e)}),[c,w,o]),tabValues:o}}var v=a(72389);const d="tabList__CuJ",y="tabItem_LNqP";function h(e){let{className:t,block:a,selectedValue:l,selectValue:i,tabValues:c}=e;const u=[],{blockElementScrollPositionUntilNextRender:p}=(0,s.o5)(),m=e=>{const t=e.currentTarget,a=u.indexOf(t),n=c[a].value;n!==l&&(p(t),i(n))},b=e=>{var t;let a=null;switch(e.key){case"Enter":m(e);break;case"ArrowRight":{const t=u.indexOf(e.currentTarget)+1;a=u[t]??u[0];break}case"ArrowLeft":{const t=u.indexOf(e.currentTarget)-1;a=u[t]??u[u.length-1];break}}null==(t=a)||t.focus()};return r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.Z)("tabs",{"tabs--block":a},t)},c.map((e=>{let{value:t,label:a,attributes:s}=e;return r.createElement("li",(0,n.Z)({role:"tab",tabIndex:l===t?0:-1,"aria-selected":l===t,key:t,ref:e=>u.push(e),onKeyDown:b,onClick:m},s,{className:(0,o.Z)("tabs__item",y,null==s?void 0:s.className,{"tabs__item--active":l===t})}),a??t)})))}function g(e){let{lazy:t,children:a,selectedValue:n}=e;const o=(Array.isArray(a)?a:[a]).filter(Boolean);if(t){const e=o.find((e=>e.props.value===n));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return r.createElement("div",{className:"margin-top--md"},o.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==n}))))}function k(e){const t=w(e);return r.createElement("div",{className:(0,o.Z)("tabs-container",d)},r.createElement(h,(0,n.Z)({},e,t)),r.createElement(g,(0,n.Z)({},e,t)))}function V(e){const t=(0,v.Z)();return r.createElement(k,(0,n.Z)({key:String(t)},e))}},31989:(e,t,a)=>{a.d(t,{Z:()=>o});var n=a(67294),r=a(52263);const o=function(e){const{className:t,py:a,scala:o,csharp:s,sourceLink:l}=e,i=(0,r.Z)().siteConfig.customFields.version;let c=`https://mmlspark.blob.core.windows.net/docs/${i}/pyspark/${a}`,u=`https://mmlspark.blob.core.windows.net/docs/${i}/scala/${o}`;return n.createElement("table",null,n.createElement("tbody",null,n.createElement("tr",null,n.createElement("td",null,n.createElement("strong",null,"Python API: "),n.createElement("a",{href:c},t)),n.createElement("td",null,n.createElement("strong",null,"Scala API: "),n.createElement("a",{href:u},t)),n.createElement("td",null,n.createElement("strong",null,"Source: "),n.createElement("a",{href:l},t)))))}},33154:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>f,contentTitle:()=>m,default:()=>d,frontMatter:()=>p,metadata:()=>b,toc:()=>w});var n=a(83117),r=(a(67294),a(3905)),o=a(74866),s=a(85162),l=a(31989);const i=[{value:"VectorZipper",id:"vectorzipper",level:2},{value:"VowpalWabbitClassifier",id:"vowpalwabbitclassifier",level:2},{value:"VowpalWabbitFeaturizer",id:"vowpalwabbitfeaturizer",level:2},{value:"VowpalWabbitInteractions",id:"vowpalwabbitinteractions",level:2}],c={toc:i};function u(e){let{components:t,...a}=e;return(0,r.kt)("wrapper",(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"vectorzipper"},"VectorZipper"),(0,r.kt)(o.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"},{label:".NET",value:"csharp"}],mdxType:"Tabs"},(0,r.kt)(s.Z,{value:"py",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import *\n\ndf = spark.createDataFrame([\n      ("action1_f", "action2_f"),\n      ("action1_f", "action2_f"),\n      ("action1_f", "action2_f"),\n      ("action1_f", "action2_f")\n], ["action1", "action2"])\n\nactionOneFeaturizer = (VowpalWabbitFeaturizer()\n    .setInputCols(["action1"])\n    .setOutputCol("sequence_one"))\n\nactionTwoFeaturizer = (VowpalWabbitFeaturizer()\n    .setInputCols(["action2"])\n    .setOutputCol("sequence_two"))\n\nseqDF = actionTwoFeaturizer.transform(actionOneFeaturizer.transform(df))\n\nvectorZipper = (VectorZipper()\n    .setInputCols(["sequence_one", "sequence_two"])\n    .setOutputCol("out"))\n\nvectorZipper.transform(seqDF).show()\n'))),(0,r.kt)(s.Z,{value:"scala",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.vw._\n\nval df = (Seq(\n      ("action1_f", "action2_f"),\n      ("action1_f", "action2_f"),\n      ("action1_f", "action2_f"),\n      ("action1_f", "action2_f")\n    ).toDF("action1", "action2"))\n\nval actionOneFeaturizer = (new VowpalWabbitFeaturizer()\n    .setInputCols(Array("action1"))\n    .setOutputCol("sequence_one"))\n\nval actionTwoFeaturizer = (new VowpalWabbitFeaturizer()\n    .setInputCols(Array("action2"))\n    .setOutputCol("sequence_two"))\n\nval seqDF = actionTwoFeaturizer.transform(actionOneFeaturizer.transform(df))\n\nval vectorZipper = (new VectorZipper()\n    .setInputCols(Array("sequence_one", "sequence_two"))\n    .setOutputCol("out"))\n\nvectorZipper.transform(seqDF).show()\n'))),(0,r.kt)(s.Z,{value:"csharp",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-csharp"},'using System;\nusing System.Collections.Generic;\nusing Synapse.ML.Vw;\nusing Microsoft.Spark.Sql;\nusing Microsoft.Spark.Sql.Types;\n\nnamespace SynapseMLApp\n{\n    class Program\n    {\n        static void Main(string[] args)\n        {\n            SparkSession spark =\n                SparkSession\n                    .Builder()\n                    .AppName("Example")\n                    .GetOrCreate();\n\n            DataFrame df = spark.CreateDataFrame(\n                new List<GenericRow>\n                {\n                    new GenericRow(new object[] {"action1_f", "action2_f"}),\n                    new GenericRow(new object[] {"action1_f", "action2_f"}),\n                    new GenericRow(new object[] {"action1_f", "action2_f"}),\n                    new GenericRow(new object[] {"action1_f", "action2_f"})\n                },\n                new StructType(new List<StructField>\n                {\n                    new StructField("action1", new StringType()),\n                    new StructField("action2", new StringType())\n                })\n            );\n\n            var actionOneFeaturizer = new VowpalWabbitFeaturizer()\n                .SetInputCols(new string[]{"action1"})\n                .SetOutputCol("sequence_one");\n            var actionTwoFeaturizer = new VowpalWabbitFeaturizer()\n                .SetInputCols(new string[]{"action2"})\n                .SetOutputCol("sequence_two");\n            var seqDF = actionTwoFeaturizer.Transform(actionOneFeaturizer.Transform(df));\n\n            var vectorZipper = new VectorZipper()\n                .SetInputCols(new string[]{"sequence_one", "sequence_two"})\n                .SetOutputCol("out");\n            vectorZipper.Transform(seqDF).Show();\n\n            spark.Stop();\n        }\n    }\n}\n')))),(0,r.kt)(l.Z,{className:"VectorZipper",py:"synapse.ml.vw.html#module-synapse.ml.vw.VectorZipper",scala:"com/microsoft/azure/synapse/ml/vw/VectorZipper.html",csharp:"classSynapse_1_1ML_1_1Vw_1_1VectorZipper.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VectorZipper.scala",mdxType:"DocTable"}),(0,r.kt)("h2",{id:"vowpalwabbitclassifier"},"VowpalWabbitClassifier"),(0,r.kt)(o.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,r.kt)(s.Z,{value:"py",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"from synapse.ml.vw import *\n\nvw = (VowpalWabbitClassifier()\n      .setNumBits(10)\n      .setLearningRate(3.1)\n      .setPowerT(0)\n      .setLabelConversion(False))\n"))),(0,r.kt)(s.Z,{value:"scala",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"import com.microsoft.azure.synapse.ml.vw._\n\nval vw = (new VowpalWabbitClassifier()\n      .setNumBits(10)\n      .setLearningRate(3.1)\n      .setPowerT(0)\n      .setLabelConversion(false))\n")))),(0,r.kt)(l.Z,{className:"VowpalWabbitClassifier",py:"synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitClassifier",scala:"com/microsoft/azure/synapse/ml/vw/VowpalWabbitClassifier.html",csharp:"classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitClassifier.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitClassifier.scala",mdxType:"DocTable"}),(0,r.kt)("h2",{id:"vowpalwabbitfeaturizer"},"VowpalWabbitFeaturizer"),(0,r.kt)(o.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,r.kt)(s.Z,{value:"py",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import *\n\nfeaturizer = (VowpalWabbitFeaturizer()\n      .setStringSplitInputCols(["in"])\n      .setPreserveOrderNumBits(2)\n      .setNumBits(18)\n      .setPrefixStringsWithColumnName(False)\n      .setOutputCol("features"))\n'))),(0,r.kt)(s.Z,{value:"scala",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.vw._\n\nval featurizer = (new VowpalWabbitFeaturizer()\n      .setStringSplitInputCols(Array("in"))\n      .setPreserveOrderNumBits(2)\n      .setNumBits(18)\n      .setPrefixStringsWithColumnName(false)\n      .setOutputCol("features"))\n')))),(0,r.kt)(l.Z,{className:"VowpalWabbitFeaturizer",py:"synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitFeaturizer",scala:"com/microsoft/azure/synapse/ml/vw/VowpalWabbitFeaturizer.html",csharp:"classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitFeaturizer.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitFeaturizer.scala",mdxType:"DocTable"}),(0,r.kt)("h2",{id:"vowpalwabbitinteractions"},"VowpalWabbitInteractions"),(0,r.kt)(o.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,r.kt)(s.Z,{value:"py",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.vw import *\n\ninteractions = (VowpalWabbitInteractions()\n    .setInputCols(["v1"])\n    .setOutputCol("out"))\n'))),(0,r.kt)(s.Z,{value:"scala",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.vw._\nimport org.apache.spark.ml.linalg._\n\ncase class Data(v1: Vector, v2: Vector, v3: Vector)\n\nval df = spark.createDataFrame(Seq(Data(\n  Vectors.dense(Array(1.0, 2.0, 3.0)),\n  Vectors.sparse(8, Array(5), Array(4.0)),\n  Vectors.sparse(11, Array(8, 9), Array(7.0, 8.0))\n)))\n\nval interactions = (new VowpalWabbitInteractions()\n    .setInputCols(Array("v1"))\n    .setOutputCol("out"))\n\ninteractions.transform(df).show()\n')))),(0,r.kt)(l.Z,{className:"VowpalWabbitInteractions",py:"synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitInteractions",scala:"com/microsoft/azure/synapse/ml/vw/VowpalWabbitInteractions.html",csharp:"classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitInteractions.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitInteractions.scala",mdxType:"DocTable"}))}u.isMDXComponent=!0;const p={title:"Transformers - Vowpal Wabbit",sidebar_label:"Vowpal Wabbit",hide_title:!0},m="Vowpal Wabbit",b={unversionedId:"Quick Examples/transformers/transformers_vw",id:"version-1.0.1/Quick Examples/transformers/transformers_vw",title:"Transformers - Vowpal Wabbit",description:"",source:"@site/versioned_docs/version-1.0.1/Quick Examples/transformers/transformers_vw.md",sourceDirName:"Quick Examples/transformers",slug:"/Quick Examples/transformers/transformers_vw",permalink:"/SynapseML/docs/1.0.1/Quick Examples/transformers/transformers_vw",draft:!1,tags:[],version:"1.0.1",frontMatter:{title:"Transformers - Vowpal Wabbit",sidebar_label:"Vowpal Wabbit",hide_title:!0}},f={},w=[...i],v={toc:w};function d(e){let{components:t,...a}=e;return(0,r.kt)("wrapper",(0,n.Z)({},v,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"vowpal-wabbit"},"Vowpal Wabbit"),(0,r.kt)(u,{mdxType:"VW"}))}d.isMDXComponent=!0}}]);