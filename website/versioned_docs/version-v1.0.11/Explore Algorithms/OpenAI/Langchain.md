---
title: Langchain
hide_title: true
status: stable
---
# Using the LangChain Transformer

LangChain is a software development framework designed to simplify the creation of applications using large language models (LLMs). Chains in LangChain go beyond just a single LLM call and are sequences of calls (can be a call to an LLM or a different utility), automating the execution of a series of calls and actions.
To make it easier to scale up the LangChain execution on a large dataset, we have integrated LangChain with the distributed machine learning library [SynapseML](https://www.microsoft.com/en-us/research/blog/synapseml-a-simple-multilingual-and-massively-parallel-machine-learning-library/). This integration makes it easy to use the [Apache Spark](https://spark.apache.org/) distributed computing framework to process millions of data with the LangChain Framework.

This tutorial shows how to apply LangChain at scale for paper summarization and organization. We start with a table of arxiv links and apply the LangChain Transformerto automatically extract the corresponding paper title, authors, summary, and some related works.

## Step 1: Prerequisites

The key prerequisites for this quickstart include a working Azure OpenAI resource, and an Apache Spark cluster with SynapseML installed. We suggest creating a Synapse workspace, but an Azure Databricks, HDInsight, or Spark on Kubernetes, or even a python environment with the `pyspark` package will work. 

1. An Azure OpenAI resource – request access [here](https://customervoice.microsoft.com/Pages/ResponsePage.aspx?id=v4j5cvGGr0GRqy180BHbR7en2Ais5pxKtso_Pz4b1_xUOFA5Qk1UWDRBMjg0WFhPMkIzTzhKQ1dWNyQlQCN0PWcu) before [creating a resource](https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource)
1. [Create a Synapse workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace)
1. [Create a serverless Apache Spark pool](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-analyze-spark#create-a-serverless-apache-spark-pool)

## Step 2: Import this guide as a notebook

The next step is to add this code into your Spark cluster. You can either create a notebook in your Spark platform and copy the code into this notebook to run the demo. Or download the notebook and import it into Synapse Analytics

1. Import the notebook into [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook), [Synapse Workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-development-using-notebooks#create-a-notebook) or if using Databricks into the [Databricks Workspace](https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-manage#create-a-notebook).
1. Install SynapseML on your cluster. Please see the installation instructions for Synapse at the bottom of [the SynapseML website](https://microsoft.github.io/SynapseML/). Note that this requires pasting an additional cell at the top of the notebook you just imported.
1. Connect your notebook to a cluster and follow along, editing and running the cells below.


```python
%pip install openai==0.28.1 langchain==0.0.331 pdf2image pdfminer.six unstructured==0.10.24 pytesseract numpy==1.22.4 nltk==3.8.1
```


```python
import os, openai, langchain, uuid
from langchain.llms import AzureOpenAI, OpenAI
from langchain.agents import load_tools, initialize_agent, AgentType
from langchain.chains import TransformChain, LLMChain, SimpleSequentialChain
from langchain.document_loaders import OnlinePDFLoader
from langchain.tools.bing_search.tool import BingSearchRun, BingSearchAPIWrapper
from langchain.prompts import PromptTemplate
from synapse.ml.services.langchain import LangchainTransformer
from synapse.ml.core.platform import running_on_synapse, find_secret
```

##  Step 3: Fill in the service information and construct the LLM
Next, please edit the cell in the notebook to point to your service. In particular set the `model_name`, `deployment_name`, `openai_api_base`, and `open_api_key` variables to match those for your OpenAI service. Please feel free to replace `find_secret` with your key as follows

`openai_api_key = "99sj2w82o...."`

`bing_subscription_key = "..."`

Note that you also need to set up your Bing search to gain access to your [Bing Search subscription key](https://learn.microsoft.com/en-us/bing/search-apis/bing-web-search/create-bing-search-service-resource).


```python
openai_api_key = find_secret(
    secret_name="openai-api-key-2", keyvault="mmlspark-build-keys"
)
openai_api_base = "https://synapseml-openai-2.openai.azure.com/"
openai_api_version = "2022-12-01"
openai_api_type = "azure"
deployment_name = "gpt-35-turbo"
bing_search_url = "https://api.bing.microsoft.com/v7.0/search"
bing_subscription_key = find_secret(
    secret_name="bing-search-key", keyvault="mmlspark-build-keys"
)

os.environ["BING_SUBSCRIPTION_KEY"] = bing_subscription_key
os.environ["BING_SEARCH_URL"] = bing_search_url
os.environ["OPENAI_API_TYPE"] = openai_api_type
os.environ["OPENAI_API_VERSION"] = openai_api_version
os.environ["OPENAI_API_BASE"] = openai_api_base
os.environ["OPENAI_API_KEY"] = openai_api_key

llm = AzureOpenAI(
    deployment_name=deployment_name,
    model_name=deployment_name,
    temperature=0.1,
    verbose=True,
)
```

## Step 4: Basic Usage of LangChain Transformer

### Create a chain
We will start by demonstrating the basic usage with a simple chain that creates definitions for input words


```python
copy_prompt = PromptTemplate(
    input_variables=["technology"],
    template="Define the following word: {technology}",
)

chain = LLMChain(llm=llm, prompt=copy_prompt)
transformer = (
    LangchainTransformer()
    .setInputCol("technology")
    .setOutputCol("definition")
    .setChain(chain)
    .setSubscriptionKey(openai_api_key)
    .setUrl(openai_api_base)
)
```

### Create a dataset and apply the chain


```python
# construction of test dataframe
df = spark.createDataFrame(
    [(0, "docker"), (1, "spark"), (2, "python")], ["label", "technology"]
)
display(transformer.transform(df))
```

### Save and load the LangChain transformer
LangChain Transformers can be saved and loaded. Note that LangChain serialization only works for chains that don't have memory.


```python
temp_dir = "tmp"
if not os.path.exists(temp_dir):
    os.mkdir(temp_dir)
path = os.path.join(temp_dir, "langchainTransformer")
transformer.save(path)
loaded = LangchainTransformer.load(path)
display(loaded.transform(df))
```

## Step 5: Using LangChain for Large scale literature review

### Create a Sequential Chain for paper summarization

We will now construct a Sequential Chain for extracting structured information from an arxiv link. In particular, we will ask langchain to extract the title, author information, and a summary of the paper content. After that, we use a web search tool to find the recent papers written by the first author.

To summarize, our sequential chain contains the following steps:

1. **Transform Chain**: Extract Paper Content from arxiv Link **=>**
1. **LLMChain**: Summarize the Paper, extract paper title and authors **=>**
1. **Transform Chain**: to generate the prompt **=>**
1. **Agent with Web Search Tool**: Use Web Search to find the recent papers by the first author


```python
def paper_content_extraction(inputs: dict) -> dict:
    arxiv_link = inputs["arxiv_link"]
    loader = OnlinePDFLoader(arxiv_link)
    pages = loader.load_and_split()
    return {"paper_content": pages[0].page_content + pages[1].page_content}


def prompt_generation(inputs: dict) -> dict:
    output = inputs["Output"]
    prompt = (
        "find the paper title, author, summary in the paper description below, output them. After that, Use websearch to find out 3 recent papers of the first author in the author section below (first author is the first name separated by comma) and list the paper titles in bullet points: <Paper Description Start>\n"
        + output
        + "<Paper Description End>."
    )
    return {"prompt": prompt}


paper_content_extraction_chain = TransformChain(
    input_variables=["arxiv_link"],
    output_variables=["paper_content"],
    transform=paper_content_extraction,
    verbose=False,
)

paper_summarizer_template = """You are a paper summarizer, given the paper content, it is your job to summarize the     paper into a short summary, and extract authors and paper title from the paper content.
Here is the paper content:
{paper_content}
Output:
paper title, authors and summary.
"""
prompt = PromptTemplate(
    input_variables=["paper_content"], template=paper_summarizer_template
)
summarize_chain = LLMChain(llm=llm, prompt=prompt, verbose=False)

prompt_generation_chain = TransformChain(
    input_variables=["Output"],
    output_variables=["prompt"],
    transform=prompt_generation,
    verbose=False,
)

bing = BingSearchAPIWrapper(k=3)
tools = [BingSearchRun(api_wrapper=bing)]
web_search_agent = initialize_agent(
    tools, llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=False
)

sequential_chain = SimpleSequentialChain(
    chains=[
        paper_content_extraction_chain,
        summarize_chain,
        prompt_generation_chain,
        web_search_agent,
    ]
)
```

### Apply the LangChain transformer to perform this workload at scale

We can now use our chain at scale using the `LangchainTransformer`


```python
paper_df = spark.createDataFrame(
    [
        (0, "https://arxiv.org/pdf/2107.13586.pdf"),
        (1, "https://arxiv.org/pdf/2101.00190.pdf"),
        (2, "https://arxiv.org/pdf/2103.10385.pdf"),
        (3, "https://arxiv.org/pdf/2110.07602.pdf"),
    ],
    ["label", "arxiv_link"],
)

# construct langchain transformer using the paper summarizer chain define above
paper_info_extractor = (
    LangchainTransformer()
    .setInputCol("arxiv_link")
    .setOutputCol("paper_info")
    .setChain(sequential_chain)
    .setSubscriptionKey(openai_api_key)
    .setUrl(openai_api_base)
)


# extract paper information from arxiv links, the paper information needs to include:
# paper title, paper authors, brief paper summary, and recent papers published by the first author
display(paper_info_extractor.transform(paper_df))
```
