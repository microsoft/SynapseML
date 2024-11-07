"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[64128],{3905:(e,t,n)=>{n.d(t,{Zo:()=>m,kt:()=>u});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var p=a.createContext({}),l=function(e){var t=a.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},m=function(e){var t=l(e.components);return a.createElement(p.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},h=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,p=e.parentName,m=s(e,["components","mdxType","originalType","parentName"]),h=l(n),u=r,d=h["".concat(p,".").concat(u)]||h[u]||c[u]||o;return n?a.createElement(d,i(i({ref:t},m),{},{components:n})):a.createElement(d,i({ref:t},m))}));function u(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=h;var s={};for(var p in t)hasOwnProperty.call(t,p)&&(s[p]=t[p]);s.originalType=e,s.mdxType="string"==typeof e?e:r,i[1]=s;for(var l=2;l<o;l++)i[l]=n[l];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}h.displayName="MDXCreateElement"},14449:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>i,default:()=>c,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var a=n(83117),r=(n(67294),n(3905));const o={title:"OpenAI",hide_title:!0,status:"stable"},i="Azure OpenAI for big data",s={unversionedId:"Explore Algorithms/OpenAI/OpenAI",id:"version-1.0.7/Explore Algorithms/OpenAI/OpenAI",title:"OpenAI",description:"The Azure OpenAI service can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples, we have integrated the Azure OpenAI service with the distributed machine learning library SynapseML. This integration makes it easy to use the Apache Spark distributed computing framework to process millions of prompts with the OpenAI service. This tutorial shows how to apply large language models at a distributed scale using Azure OpenAI.",source:"@site/versioned_docs/version-1.0.7/Explore Algorithms/OpenAI/OpenAI.md",sourceDirName:"Explore Algorithms/OpenAI",slug:"/Explore Algorithms/OpenAI/",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/OpenAI/",draft:!1,tags:[],version:"1.0.7",frontMatter:{title:"OpenAI",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Langchain",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/OpenAI/Langchain"},next:{title:"Quickstart - OpenAI Embedding",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/OpenAI/Quickstart - OpenAI Embedding"}},p={},l=[{value:"Prerequisites",id:"prerequisites",level:2},{value:"Import this guide as a notebook",id:"import-this-guide-as-a-notebook",level:2},{value:"Fill in service information",id:"fill-in-service-information",level:2},{value:"Create a dataset of prompts",id:"create-a-dataset-of-prompts",level:2},{value:"Create the OpenAICompletion Apache Spark Client",id:"create-the-openaicompletion-apache-spark-client",level:2},{value:"Transform the dataframe with the OpenAICompletion Client",id:"transform-the-dataframe-with-the-openaicompletion-client",level:2},{value:"More Usage Examples",id:"more-usage-examples",level:2},{value:"Generating Text Embeddings",id:"generating-text-embeddings",level:3},{value:"Chat Completion",id:"chat-completion",level:3},{value:"Improve throughput with request batching",id:"improve-throughput-with-request-batching",level:3},{value:"Using an automatic minibatcher",id:"using-an-automatic-minibatcher",level:3},{value:"Prompt engineering for translation",id:"prompt-engineering-for-translation",level:3},{value:"Prompt for question answering",id:"prompt-for-question-answering",level:3}],m={toc:l};function c(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},m,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"azure-openai-for-big-data"},"Azure OpenAI for big data"),(0,r.kt)("p",null,"The Azure OpenAI service can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples, we have integrated the Azure OpenAI service with the distributed machine learning library ",(0,r.kt)("a",{parentName:"p",href:"https://www.microsoft.com/en-us/research/blog/synapseml-a-simple-multilingual-and-massively-parallel-machine-learning-library/"},"SynapseML"),". This integration makes it easy to use the ",(0,r.kt)("a",{parentName:"p",href:"https://spark.apache.org/"},"Apache Spark")," distributed computing framework to process millions of prompts with the OpenAI service. This tutorial shows how to apply large language models at a distributed scale using Azure OpenAI. "),(0,r.kt)("h2",{id:"prerequisites"},"Prerequisites"),(0,r.kt)("p",null,"The key prerequisites for this quickstart include a working Azure OpenAI resource, and an Apache Spark cluster with SynapseML installed. We suggest creating a Synapse workspace, but an Azure Databricks, HDInsight, or Spark on Kubernetes, or even a python environment with the ",(0,r.kt)("inlineCode",{parentName:"p"},"pyspark")," package will work. "),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},"An Azure OpenAI resource \u2013 request access ",(0,r.kt)("a",{parentName:"li",href:"https://customervoice.microsoft.com/Pages/ResponsePage.aspx?id=v4j5cvGGr0GRqy180BHbR7en2Ais5pxKtso_Pz4b1_xUOFA5Qk1UWDRBMjg0WFhPMkIzTzhKQ1dWNyQlQCN0PWcu"},"here")," before ",(0,r.kt)("a",{parentName:"li",href:"https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource"},"creating a resource")),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("a",{parentName:"li",href:"https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace"},"Create a Synapse workspace")),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("a",{parentName:"li",href:"https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-analyze-spark#create-a-serverless-apache-spark-pool"},"Create a serverless Apache Spark pool"))),(0,r.kt)("h2",{id:"import-this-guide-as-a-notebook"},"Import this guide as a notebook"),(0,r.kt)("p",null,"The next step is to add this code into your Spark cluster. You can either create a notebook in your Spark platform and copy the code into this notebook to run the demo. Or download the notebook and import it into Synapse Analytics"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"https://github.com/microsoft/SynapseML/blob/master/docs/Explore%20Algorithms/OpenAI/OpenAI.ipynb"},"Download this demo as a notebook")," (select ",(0,r.kt)("strong",{parentName:"li"},"Raw"),", then save the file)"),(0,r.kt)("li",{parentName:"ul"},"Import the notebook. ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"If you are using Synapse Analytics ",(0,r.kt)("a",{parentName:"li",href:"https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-development-using-notebooks#create-a-notebook"},"into the Synapse Workspace")," "),(0,r.kt)("li",{parentName:"ul"},"If your are using Databricks ",(0,r.kt)("a",{parentName:"li",href:"https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-manage#create-a-notebook"},"import into the Databricks Workspace"),". "),(0,r.kt)("li",{parentName:"ul"},"If you are using Fabric ",(0,r.kt)("a",{parentName:"li",href:"https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook"},"import into the Fabric Workspace")))),(0,r.kt)("li",{parentName:"ul"},"Install SynapseML on your cluster. See the installation instructions for Synapse at the bottom of ",(0,r.kt)("a",{parentName:"li",href:"https://microsoft.github.io/SynapseML/"},"the SynapseML website"),". ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"If you are using Fabric, please check ",(0,r.kt)("a",{parentName:"li",href:"https://learn.microsoft.com/en-us/fabric/data-science/install-synapseml"},"Installation Guide"),". This requires pasting an extra cell at the top of the notebook you imported. "))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"Connect your notebook to a cluster and follow along, editing and running the cells.\n")))),(0,r.kt)("h2",{id:"fill-in-service-information"},"Fill in service information"),(0,r.kt)("p",null,"Next, edit the cell in the notebook to point to your service. In particular set the ",(0,r.kt)("inlineCode",{parentName:"p"},"service_name"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"deployment_name"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"location"),", and ",(0,r.kt)("inlineCode",{parentName:"p"},"key")," variables to match them to your OpenAI service:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.core.platform import find_secret\n\n# Fill in the following lines with your service information\n# Learn more about selecting which embedding model to choose: https://openai.com/blog/new-and-improved-embedding-model\nservice_name = "synapseml-openai-2"\ndeployment_name = "gpt-35-turbo"\ndeployment_name_embeddings = "text-embedding-ada-002"\n\nkey = find_secret(\n    secret_name="openai-api-key-2", keyvault="mmlspark-build-keys"\n)  # please replace this line with your key as a string\n\nassert key is not None and service_name is not None\n')),(0,r.kt)("h2",{id:"create-a-dataset-of-prompts"},"Create a dataset of prompts"),(0,r.kt)("p",null,"Next, create a dataframe consisting of a series of rows, with one prompt per row. "),(0,r.kt)("p",null,"You can also load data directly from ADLS or other databases. For more information on loading and preparing Spark dataframes, see the ",(0,r.kt)("a",{parentName:"p",href:"https://spark.apache.org/docs/latest/sql-data-sources.html"},"Apache Spark data loading guide"),"."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    [\n        ("Hello my name is",),\n        ("The best code is code thats",),\n        ("SynapseML is ",),\n    ]\n).toDF("prompt")\n')),(0,r.kt)("h2",{id:"create-the-openaicompletion-apache-spark-client"},"Create the OpenAICompletion Apache Spark Client"),(0,r.kt)("p",null,"To apply the OpenAI Completion service to your dataframe you created, create an OpenAICompletion object, which serves as a distributed client. Parameters of the service can be set either with a single value, or by a column of the dataframe with the appropriate setters on the ",(0,r.kt)("inlineCode",{parentName:"p"},"OpenAICompletion")," object. Here we're setting ",(0,r.kt)("inlineCode",{parentName:"p"},"maxTokens")," to 200. A token is around four characters, and this limit applies to the sum of the prompt and the result. We're also setting the ",(0,r.kt)("inlineCode",{parentName:"p"},"promptCol")," parameter with the name of the prompt column in the dataframe."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services.openai import OpenAICompletion\n\ncompletion = (\n    OpenAICompletion()\n    .setSubscriptionKey(key)\n    .setDeploymentName(deployment_name)\n    .setCustomServiceName(service_name)\n    .setMaxTokens(200)\n    .setPromptCol("prompt")\n    .setErrorCol("error")\n    .setOutputCol("completions")\n)\n')),(0,r.kt)("h2",{id:"transform-the-dataframe-with-the-openaicompletion-client"},"Transform the dataframe with the OpenAICompletion Client"),(0,r.kt)("p",null,"After creating the dataframe and the completion client, you can transform your input dataset and add a column called ",(0,r.kt)("inlineCode",{parentName:"p"},"completions")," with all of the information the service adds. Select just the text for simplicity."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.sql.functions import col\n\ncompleted_df = completion.transform(df).cache()\ndisplay(\n    completed_df.select(\n        col("prompt"),\n        col("error"),\n        col("completions.choices.text").getItem(0).alias("text"),\n    )\n)\n')),(0,r.kt)("p",null,"Your output should look something like this. The completion text will be different from the sample."),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:"center"},(0,r.kt)("strong",{parentName:"th"},"prompt")),(0,r.kt)("th",{parentName:"tr",align:"center"},(0,r.kt)("strong",{parentName:"th"},"error")),(0,r.kt)("th",{parentName:"tr",align:"center"},(0,r.kt)("strong",{parentName:"th"},"text")))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"Hello my name is"),(0,r.kt)("td",{parentName:"tr",align:"center"},"null"),(0,r.kt)("td",{parentName:"tr",align:"center"},"Makaveli I'm eighteen years old and I want to   be a rapper when I grow up I love writing and making music I'm from Los   Angeles, CA")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"The best code is code thats"),(0,r.kt)("td",{parentName:"tr",align:"center"},"null"),(0,r.kt)("td",{parentName:"tr",align:"center"},"understandable This is a subjective statement,   and there is no definitive answer.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:"center"},"SynapseML is"),(0,r.kt)("td",{parentName:"tr",align:"center"},"null"),(0,r.kt)("td",{parentName:"tr",align:"center"},"A machine learning algorithm that is able to learn how to predict the future outcome of events.")))),(0,r.kt)("h2",{id:"more-usage-examples"},"More Usage Examples"),(0,r.kt)("h3",{id:"generating-text-embeddings"},"Generating Text Embeddings"),(0,r.kt)("p",null,"In addition to completing text, we can also embed text for use in downstream algorithms or vector retrieval architectures. Creating embeddings allows you to search and retrieve documents from large collections and can be used when prompt engineering isn't sufficient for the task."),(0,r.kt)("p",null,"For more information on using ",(0,r.kt)("inlineCode",{parentName:"p"},"OpenAIEmbedding")," see our ",(0,r.kt)("a",{parentName:"p",href:"./Quickstart%20-%20OpenAI%20Embedding"},"embedding guide"),"."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services.openai import OpenAIEmbedding\n\nembedding = (\n    OpenAIEmbedding()\n    .setSubscriptionKey(key)\n    .setDeploymentName(deployment_name_embeddings)\n    .setCustomServiceName(service_name)\n    .setTextCol("prompt")\n    .setErrorCol("error")\n    .setOutputCol("embeddings")\n)\n\ndisplay(embedding.transform(df))\n')),(0,r.kt)("h3",{id:"chat-completion"},"Chat Completion"),(0,r.kt)("p",null,"Models such as ChatGPT and GPT-4 are capable of understanding chats instead of single prompts. The ",(0,r.kt)("inlineCode",{parentName:"p"},"OpenAIChatCompletion")," transformer exposes this functionality at scale."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services.openai import OpenAIChatCompletion\nfrom pyspark.sql import Row\nfrom pyspark.sql.types import *\n\n\ndef make_message(role, content):\n    return Row(role=role, content=content, name=role)\n\n\nchat_df = spark.createDataFrame(\n    [\n        (\n            [\n                make_message(\n                    "system", "You are an AI chatbot with red as your favorite color"\n                ),\n                make_message("user", "Whats your favorite color"),\n            ],\n        ),\n        (\n            [\n                make_message("system", "You are very excited"),\n                make_message("user", "How are you today"),\n            ],\n        ),\n    ]\n).toDF("messages")\n\n\nchat_completion = (\n    OpenAIChatCompletion()\n    .setSubscriptionKey(key)\n    .setDeploymentName(deployment_name)\n    .setCustomServiceName(service_name)\n    .setMessagesCol("messages")\n    .setErrorCol("error")\n    .setOutputCol("chat_completions")\n)\n\ndisplay(\n    chat_completion.transform(chat_df).select(\n        "messages", "chat_completions.choices.message.content"\n    )\n)\n')),(0,r.kt)("h3",{id:"improve-throughput-with-request-batching"},"Improve throughput with request batching"),(0,r.kt)("p",null,'The example makes several requests to the service, one for each prompt. To complete multiple prompts in a single request, use batch mode. First, in the OpenAICompletion object, instead of setting the Prompt column to "Prompt", specify "batchPrompt" for the BatchPrompt column.\nTo do so, create a dataframe with a list of prompts per row.'),(0,r.kt)("p",null,'As of this writing there\'s currently a limit of 20 prompts in a single request, and a hard limit of 2048 "tokens", or approximately 1500 words.'),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'batch_df = spark.createDataFrame(\n    [\n        (["The time has come", "Pleased to", "Today stocks", "Here\'s to"],),\n        (["The only thing", "Ask not what", "Every litter", "I am"],),\n    ]\n).toDF("batchPrompt")\n')),(0,r.kt)("p",null,"Next we create the OpenAICompletion object. Rather than setting the prompt column, set the batchPrompt column if your column is of type ",(0,r.kt)("inlineCode",{parentName:"p"},"Array[String]"),"."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'batch_completion = (\n    OpenAICompletion()\n    .setSubscriptionKey(key)\n    .setDeploymentName(deployment_name)\n    .setCustomServiceName(service_name)\n    .setMaxTokens(200)\n    .setBatchPromptCol("batchPrompt")\n    .setErrorCol("error")\n    .setOutputCol("completions")\n)\n')),(0,r.kt)("p",null,"In the call to transform, a request will be made per row. Since there are multiple prompts in a single row, each request is sent with all prompts in that row. The results contain a row for each row in the request."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"completed_batch_df = batch_completion.transform(batch_df).cache()\ndisplay(completed_batch_df)\n")),(0,r.kt)("h3",{id:"using-an-automatic-minibatcher"},"Using an automatic minibatcher"),(0,r.kt)("p",null,"If your data is in column format, you can transpose it to row format using SynapseML's ",(0,r.kt)("inlineCode",{parentName:"p"},"FixedMiniBatcherTransformer"),"."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.sql.types import StringType\nfrom synapse.ml.stages import FixedMiniBatchTransformer\nfrom synapse.ml.core.spark import FluentAPI\n\ncompleted_autobatch_df = (\n    df.coalesce(\n        1\n    )  # Force a single partition so that our little 4-row dataframe makes a batch of size 4, you can remove this step for large datasets\n    .mlTransform(FixedMiniBatchTransformer(batchSize=4))\n    .withColumnRenamed("prompt", "batchPrompt")\n    .mlTransform(batch_completion)\n)\n\ndisplay(completed_autobatch_df)\n')),(0,r.kt)("h3",{id:"prompt-engineering-for-translation"},"Prompt engineering for translation"),(0,r.kt)("p",null,"The Azure OpenAI service can solve many different natural language tasks through ",(0,r.kt)("a",{parentName:"p",href:"https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/completions"},"prompt engineering"),". Here, we show an example of prompting for language translation:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'translate_df = spark.createDataFrame(\n    [\n        ("Japanese: Ookina hako \\nEnglish: Big box \\nJapanese: Midori tako\\nEnglish:",),\n        (\n            "French: Quel heure et il au Montreal? \\nEnglish: What time is it in Montreal? \\nFrench: Ou est le poulet? \\nEnglish:",\n        ),\n    ]\n).toDF("prompt")\n\ndisplay(completion.transform(translate_df))\n')),(0,r.kt)("h3",{id:"prompt-for-question-answering"},"Prompt for question answering"),(0,r.kt)("p",null,"Here, we prompt GPT-3 for general-knowledge question answering:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'qa_df = spark.createDataFrame(\n    [\n        (\n            "Q: Where is the Grand Canyon?\\nA: The Grand Canyon is in Arizona.\\n\\nQ: What is the weight of the Burj Khalifa in kilograms?\\nA:",\n        )\n    ]\n).toDF("prompt")\n\ndisplay(completion.transform(qa_df))\n')))}c.isMDXComponent=!0}}]);