---
title: Introduction
description: MLflow support of SynapseML
---

## What is MLflow

[MLflow](https://github.com/mlflow/mlflow) is a platform to streamline machine learning development, including tracking experiments, packaging code into reproducible runs, and sharing and deploying models. MLflow offers a set of lightweight APIs that can be used with any existing machine learning application or library (TensorFlow, PyTorch, XGBoost, etc), wherever you currently run ML code (e.g. in notebooks, standalone applications or the cloud). MLflow's current components are:

* [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html): An API to log parameters, code, and results in machine learning experiments and compare them using an interactive UI.
* [MLflow Projects](https://mlflow.org/docs/latest/projects.html): A code packaging format for reproducible runs using Conda and Docker, so you can share your ML code with others.
* [MLflow Models](https://mlflow.org/docs/latest/models.html): A model packaging format and tools that let you easily deploy the same model (from any ML library) to batch and real-time scoring on platforms such as Docker, Apache Spark, Azure ML and AWS SageMaker.
* [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html): A centralized model store, set of APIs, and UI, to collaboratively manage the full lifecycle of MLflow Models.

## Installation

Install MLflow from PyPI via `pip install mlflow`

MLflow requires `conda` to be on the `PATH` for the projects feature.

Learn more about MLflow on their [GitHub page](https://github.com/mlflow/mlflow).
