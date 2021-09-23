---
title: DeepLearning - BiLSTM Medical Entity Extraction
hide_title: true
type: notebook
status: stable
categories: ["Deep Learning"]
---

## DeepLearning - BiLSTM Medical Entity Extraction

In this tutorial we use a Bidirectional LSTM entity extractor from the MMLSPark
model downloader to extract entities from PubMed medical abstracts

Our goal is to identify useful entities in a block of free-form text.  This is a
nontrivial task because entities might be referenced in the text using variety of
synonymns, abbreviations, or formats. Our target output for this model is a set
of tags that specify what kind of entity is referenced. The model we use was
trained on a large dataset of publically tagged pubmed abstracts. An example
annotated sequence is given below, "O" represents no tag:

|I-Chemical | O   |I-Chemical  | O   | O   |I-Chemical | O   |I-Chemical  | O   | O      | O   | O   |I-Disease |I-Disease| O   | O    |
|:---:      |:---:|:---:       |:---:|:---:|:---:      |:---:|:---:       |:---:|:---:   |:---:|:---:|:---:     |:---:    |:---:|:---: |
|Baricitinib| ,   |Methotrexate| ,   | or  |Baricitinib|Plus |Methotrexate| in  |Patients|with |Early|Rheumatoid|Arthritis| Who |Had...|




```python
from mmlspark.cntk import CNTKModel
from mmlspark.downloader import ModelDownloader
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, ArrayType, FloatType, StringType
from pyspark.sql import Row

from os.path import abspath, join
import numpy as np
from nltk.tokenize import sent_tokenize, word_tokenize
import os, tarfile, pickle
import urllib.request
import nltk
```

Get the model and extract the data.


```python
modelName = "BiLSTM"
modelDir = "models"
if not os.path.exists(modelDir): os.makedirs(modelDir)
d = ModelDownloader(spark, "dbfs:///" + modelDir)
modelSchema = d.downloadByName(modelName)
nltk.download("punkt", "/dbfs/nltkdata")
nltk.data.path.append("/dbfs/nltkdata")
```


```python
modelName = "BiLSTM"
modelDir = abspath("models")
if not os.path.exists(modelDir): os.makedirs(modelDir)
d = ModelDownloader(spark, "file://" + modelDir)
modelSchema = d.downloadByName(modelName)
nltk.download("punkt")
```

Download the embeddings

We use the nltk punkt sentence and word tokenizers and a set of embeddings trained on PubMed Articles


```python
wordEmbFileName = "WordEmbeddings_PubMed.pkl"
pickleFile = join(abspath("models"), wordEmbFileName)
if not os.path.isfile(pickleFile):
    urllib.request.urlretrieve("https://mmlspark.blob.core.windows.net/datasets/" + wordEmbFileName, pickleFile)
```

Load the embeddings and create functions for encoding sentences


```python
pickleContent = pickle.load(open(pickleFile, "rb"), encoding="latin-1")
wordToIndex = pickleContent["word_to_index"]
wordvectors = pickleContent["wordvectors"]
classToEntity = pickleContent["class_to_entity"]

nClasses = len(classToEntity)
nFeatures = wordvectors.shape[1]
maxSentenceLen = 613
```


```python
content = "Baricitinib, Methotrexate, or Baricitinib Plus Methotrexate in Patients with Early Rheumatoid\
 Arthritis Who Had Received Limited or No Treatment with Disease-Modifying-Anti-Rheumatic-Drugs (DMARDs):\
 Phase 3 Trial Results. Keywords: Janus kinase (JAK), methotrexate (MTX) and rheumatoid arthritis (RA) and\
 Clinical research. In 2 completed phase 3 studies, baricitinib (bari) improved disease activity with a\
 satisfactory safety profile in patients (pts) with moderately-to-severely active RA who were inadequate\
 responders to either conventional synthetic1 or biologic2DMARDs. This abstract reports results from a\
 phase 3 study of bari administered as monotherapy or in combination with methotrexate (MTX) to pts with\
 early active RA who had limited or no prior treatment with DMARDs. MTX monotherapy was the active comparator."
```


```python
sentences = sent_tokenize(content)
df = spark.createDataFrame(enumerate(sentences), ["index","sentence"])
```


```python
# Add the tokenizers to all worker nodes
def prepNLTK(partition):
    nltk.data.path.append("/dbfs/nltkdata")
    return partition

df = df.rdd.mapPartitions(prepNLTK).toDF()
```


```python
def safe_tokenize(sent):
    try:
        return word_tokenize(sent)
    except LookupError:
        prepNLTK(None)
        return word_tokenize(sent)

tokenizeUDF = udf(safe_tokenize, ArrayType(StringType()))
df = df.withColumn("tokens",tokenizeUDF("sentence"))

countUDF = udf(len, IntegerType())
df = df.withColumn("count",countUDF("tokens"))

def wordToEmb(word):
    return wordvectors[wordToIndex.get(word.lower(), wordToIndex["UNK"])]

def featurize(tokens):
    X = np.zeros((maxSentenceLen, nFeatures))
    X[-len(tokens):,:] = np.array([wordToEmb(word) for word in tokens])
    return [float(x) for x in X.reshape(maxSentenceLen, nFeatures).flatten()]

def safe_show(df, retries):
    try:
        df.show()
    except Exception as e:
        if retries >= 1:
            safe_show(df, retries-1)
        else:
            raise e

featurizeUDF = udf(featurize,  ArrayType(FloatType()))

df = df.withColumn("features", featurizeUDF("tokens")).cache()
safe_show(df, 5) # Can be flaky on build server
    

```

Run the CNTKModel


```python
model = CNTKModel() \
    .setModelLocation(modelSchema.uri) \
    .setInputCol("features") \
    .setOutputCol("probs") \
    .setOutputNodeIndex(0) \
    .setMiniBatchSize(1)

df = model.transform(df).cache()
df.show()
```


```python
def probsToEntities(probs, wordCount):
    reshaped_probs = np.array(probs).reshape(maxSentenceLen, nClasses)
    reshaped_probs = reshaped_probs[-wordCount:,:]
    return [classToEntity[np.argmax(probs)] for probs in reshaped_probs]

toEntityUDF = udf(probsToEntities,ArrayType(StringType()))
df = df.withColumn("entities", toEntityUDF("probs", "count"))
df.show()
```

Show the annotated text


```python
# Color Code the Text based on the entity type
colors = {
    "B-Disease": "blue",
    "I-Disease":"blue",
    "B-Drug":"lime",
    "I-Drug":"lime",
    "B-Chemical":"lime",
    "I-Chemical":"lime",
    "O":"black",
    "NONE":"black"
}

def prettyPrint(words, annotations):
    formattedWords = []
    for word,annotation in zip(words,annotations):
        formattedWord = "<font size = '2' color = '{}'>{}</font>".format(colors[annotation], word)
        if annotation in {"O","NONE"}:
            formattedWords.append(formattedWord)
        else:
            formattedWords.append("<b>{}</b>".format(formattedWord))
    return " ".join(formattedWords)

prettyPrintUDF = udf(prettyPrint, StringType())
df = df.withColumn("formattedSentence", prettyPrintUDF("tokens", "entities")) \
       .select("formattedSentence")

sentences = [row["formattedSentence"] for row in df.collect()]
```


```python
from IPython.core.display import display, HTML
for sentence in sentences:
    display(HTML(sentence))
```

Example text used in this demo has been taken from:

Fleischmann R, Takeuchi T, Schlichting DE, Macias WL, Rooney T, Gurbuz S, Stoykov I,
Beattie SD, Kuo WL, Schiff M. Baricitinib, Methotrexate, or Baricitinib Plus Methotrexate
in Patients with Early Rheumatoid Arthritis Who Had Received Limited or No Treatment with
Disease-Modifying Anti-Rheumatic Drugs (DMARDs): Phase 3 Trial Results [abstract].
Arthritis Rheumatol. 2015; 67 (suppl 10).
http://acrabstracts.org/abstract/baricitinib-methotrexate-or-baricitinib-plus-methotrexate-in-patients-with-early-rheumatoid-arthritis-who-had-received-limited-or-no-treatment-with-disease-modifying-anti-rheumatic-drugs-dmards-p/.
Accessed August 18, 2017.
