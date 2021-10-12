---
title: SynapseML
sidebar_label: Introduction
hide_title: true
---

<div style={{textAlign: 'left'}}><img src="/img/logo.svg" /></div>

# SynapseML

MMLSpark is an ecosystem of tools aimed towards expanding the distributed computing framework
[Apache Spark](https://github.com/apache/spark) in several new directions.
MMLSpark adds many deep learning and data science tools to the Spark ecosystem,
including seamless integration of Spark Machine Learning pipelines with [Microsoft Cognitive Toolkit
(CNTK)](https://github.com/Microsoft/CNTK), [LightGBM](https://github.com/Microsoft/LightGBM) and
[OpenCV](http://www.opencv.org/). These tools enable powerful and highly-scalable predictive and analytical models
for a variety of datasources.

MMLSpark also brings new networking capabilities to the Spark Ecosystem. With the HTTP on Spark project, users
can embed **any** web service into their SparkML models. In this vein, MMLSpark provides easy to use
SparkML transformers for a wide variety of [Microsoft Cognitive Services](https://azure.microsoft.com/en-us/services/cognitive-services/). For production grade deployment, the Spark Serving project enables high throughput,
sub-millisecond latency web services, backed by your Spark cluster.

MMLSpark requires Scala 2.11, Spark 2.4+, and Python 3.5+.
See the API documentation [for
Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc4/scala/index.html#package) and [for
PySpark](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc4/pyspark/index.html).

import Link from '@docusaurus/Link';

<Link to="/docs/getting_started/installation" className="button button--lg button--outline button--block button--primary">Get Started</Link>

## Examples

import NotebookExamples from "@theme/NotebookExamples";

<NotebookExamples/>

## Explore our Features

import FeatureCards from "@theme/FeatureCards";

<FeatureCards/>

## Papers

- [Large Scale Intelligent Microservices](https://arxiv.org/abs/2009.08044)

- [Conditional Image Retrieval](https://arxiv.org/abs/2007.07177)

- [MMLSpark: Unifying Machine Learning Ecosystems at Massive Scales](https://arxiv.org/abs/1810.08744)

- [Flexible and Scalable Deep Learning with MMLSpark](https://arxiv.org/abs/1804.04031)
