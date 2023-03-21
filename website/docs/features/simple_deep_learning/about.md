---
title: Simple Deep Learning with SynapseML
sidebar_label: About
---

### Why Simple Deep Learning
Creating a Spark-compatible deep learning system can be challenging for users who may not have a 
thorough understanding of deep learning and distributed systems. Additionally, writing custom deep learning 
scripts may be a cumbersome and time-consuming task.
SynapseML aims to simplify this process by building on top of the [Horovod](https://github.com/horovod/horovod) Estimator, a general-purpose 
distributed deep learning model that is compatible with SparkML, and [Pytorch-lightning](https://github.com/Lightning-AI/lightning),
a lightweight wrapper around the popular PyTorch deep learning framework.

SynapseML's simple deep learning toolkit makes it easy to use modern deep learning methods in Apache Spark.
By providing a collection of Estimators, SynapseML enables users to perform distributed transfer learning on
spark clusters to solve custom machine learning tasks without requiring in-depth domain expertise.
Whether you are a data scientist, data engineer, or business analyst this project aims to make modern deep-learning methods easy to use for new domain-specific problems.

### SynapseML's Simple DNN
SynapseML goes beyond the limited support for deep networks in SparkML and provides out-of-the-box solutions for various common scenarios:
- Visual Classification: Users can apply transfer learning for image classification tasks, using pre-trained models and fine-tuning them to solve custom classification problems.
- Text Classification: SynapseML simplifies the process of implementing natural language processing tasks such as sentiment analysis, text classification, and language modeling by providing pre-built models and tools.
- More to be added...

### Why Horovod
Horovod is a distributed deep learning framework developed by Uber, which has become popular for its ability to scale
deep learning tasks across multiple GPUs and compute nodes efficiently. It is designed to work with TensorFlow, Keras, 
PyTorch, and Apache MXNet.
- Scalability: Horovod uses efficient communication algorithms like ring-allreduce and hierarchical allreduce, which allow it to scale the training process across multiple GPUs and nodes without significant performance degradation.
- Easy Integration: Horovod can be easily integrated into existing deep learning codebases with minimal changes, making it a popular choice for distributed training.
- Fault Tolerance: Horovod provides fault tolerance features like elastic training, which can dynamically adapt to changes in the number of workers or recover from failures.
- Community Support: Horovod has an active community and is widely used in the industry, which ensures that the framework is continually updated and improved.

### Why Pytorch Lightning
PyTorch Lightning is a lightweight wrapper around the popular PyTorch deep learning framework, designed to make it 
easier to write clean, modular, and scalable deep learning code. PyTorch Lightning has several advantages that 
make it an excellent choice for SynapseML's Simple Deep Learning:
- Code Organization: PyTorch Lightning promotes a clean and organized code structure by separating the research code from the engineering code. This makes it easier to maintain, debug, and share deep learning models.
- Flexibility: PyTorch Lightning retains the flexibility and expressiveness of PyTorch while adding useful abstractions to simplify the training loop and other boilerplate code.
- Built-in Best Practices: PyTorch Lightning incorporates many best practices for deep learning, such as automatic optimization, gradient clipping, and learning rate scheduling, making it easier for users to achieve optimal performance.
- Compatibility: PyTorch Lightning is compatible with a wide range of popular tools and frameworks, including Horovod, which allows users to easily leverage distributed training capabilities.
- Rapid Development: With PyTorch Lightning, users can quickly prototype and experiment with different model architectures and training strategies without worrying about low-level implementation details.

### Sample usage with DeepVisionClassifier
DeepVisionClassifier incorporates all models supported by [torchvision](https://github.com/pytorch/vision). 
:::note
The current version is based on pytorch_lightning v1.5.0 and torchvision v0.12.0
:::
By providing a spark dataframe that contains an 'imageCol' and 'labelCol', you could directly apply 'transform' function
on it with DeepVisionClassifier.
```python
train_df = spark.createDataframe([
    ("PATH_TO_IMAGE_1.jpg", 1),
    ("PATH_TO_IMAGE_2.jpg", 2)
], ["image", "label"])

deep_vision_classifier = DeepVisionClassifier(
    backbone="resnet50", # Put your backbone here
    store=store, # Corresponding store
    callbacks=callbacks, # Optional callbacks
    num_classes=17,
    batch_size=16,
    epochs=epochs,
    validation=0.1,
)

deep_vision_model = deep_vision_classifier.fit(train_df)
```
DeepVisionClassifier does distributed-training on spark with Horovod under the hood, after this fitting process it returns
a DeepVisionModel. With below code you could use the model for inference directly:
```python
pred_df = deep_vision_model.transform(test_df)
```

## Examples
- [DeepLearning - Deep Vision Classification](../DeepLearning%20-%20Deep%20Vision%20Classification)
- [DeepLearning - Deep Text Classification](../DeepLearning%20-%20Deep%20Text%20Classification)
