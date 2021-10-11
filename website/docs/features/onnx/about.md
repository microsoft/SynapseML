---
title: ONNX model inferencing on Spark
hide_title: true
sidebar_label: About
description: Learn how to use the ONNX model transformer to run inference for an ONNX model on Spark.
---

# ONNX model inferencing on Spark

## ONNX

[ONNX](https://onnx.ai/) is an open format to represent both deep learning and traditional machine learning models. With ONNX, AI developers can more easily move models between state-of-the-art tools and choose the combination that is best for them.

MMLSpark now includes a Spark transformer to bring an trained ONNX model to Apache Spark, so you can run inference on your data with Spark's large-scale data processing power.

## Usage

1. Create a `com.microsoft.ml.spark.onnx.ONNXModel` object and use `setModelLocation` or `setModelPayload` to load the ONNX model.

    For example:

    ```scala
    val onnx = new ONNXModel().setModelLocation("/path/to/model.onnx")
    ```

2. Use ONNX visualization tool (e.g. [Netron](https://netron.app/)) to inspect the ONNX model's input and output nodes.

    ![Screenshot that illustrates an ONNX model's input and output nodes](https://mmlspark.blob.core.windows.net/graphics/ONNXModelInputsOutputs.png)

3. Set the parameters properly to the `ONNXModel` object.

    The `com.microsoft.ml.spark.onnx.ONNXModel` class provides a set of parameters to control the behavior of the inference.

    | Parameter         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Default Value                                  |
    |:------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------|
    | feedDict          | Map the ONNX model's expected input node names to the input DataFrame's column names. Make sure the input DataFrame's column schema matches with the corresponding input's shape of the ONNX model. For example, an image classification model may have an input node of shape `[1, 3, 224, 224]` with type Float. It is assumed that the first dimension (1) is the batch size. Then the input DataFrame's corresponding column's type should be `ArrayType(ArrayType(ArrayType(FloatType)))`. | None                                           |
    | fetchDict         | Map the output DataFrame's column names to the ONNX model's output node names.                                                                                                                                                                                                                                                                                                                                                                                                                  | None                                           |
    | miniBatcher       | Specify the MiniBatcher to use.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `FixedMiniBatchTransformer` with batch size 10 |
    | softMaxDict       | A map between output DataFrame columns, where the value column will be computed from taking the softmax of the key column. If the 'rawPrediction' column contains logits outputs, then one can set softMaxDict to `Map("rawPrediction" -> "probability")` to obtain the probability outputs.                                                                                                                                                                                                    | None                                           |
    | argMaxDict        | A map between output DataFrame columns, where the value column will be computed from taking the argmax of the key column. This can be used to convert probability or logits output to the predicted label.                                                                                                                                                                                                                                                                                      | None                                           |
    | deviceType        | Specify a device type the model inference runs on. Supported types are: CPU or CUDA. If not specified, auto detection will be used.                                                                                                                                                                                                                                                                                                                                                             | None                                           |
    | optimizationLevel | Specify the [optimization level](https://onnxruntime.ai/docs/resources/graph-optimizations.html#graph-optimization-levels) for the ONNX graph optimizations. Supported values are: `NO_OPT`, `BASIC_OPT`, `EXTENDED_OPT`, `ALL_OPT`.                                                                                                                                                                                                                                                            | `ALL_OPT`                                      |

4. Call `transform` method to run inference on the input DataFrame.

## Example

- [Interpretability - Image Explainers](/docs/examples/model_interpretability/Interpretability%20-%20Image%20Explainers)
- [ONNX - Inference on Spark](/docs/features/onnx/ONNX%20-%20Inference%20on%20Spark)
