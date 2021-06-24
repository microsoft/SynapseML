# Model Interpretation on Spark

## Interpretable Machine Learning

Interpretable Machine Learning helps developers, data scientists and business stakeholders in the organization gain a comprehensive understanding of their machine learning models. It can also be used to debug models, explain predictions and enable auditing to meet compliance with regulatory requirements.

## Why run model interpretation on Spark

Model-agnostic interpretation methods can be computationally expensive due to the multiple evaluations needed to compute the explanations. Model interpretation on Spark enables users to interpret a black-box model at massive scales with the Apache Spark™ distributed computing ecosystem. Various components support local interpretation for tabular, vector, image and text classification models, with two popular model-agnostic interpretation methods: Local Surrogate (LIME) and Kernel SHAP.

## Usage

Both LIME and Kernel SHAP are local interpretation methods. Local interpretation explains why does the model predict certain outcome for a given observation.

Both explainers extends from `org.apache.spark.ml.Transformer`. After setting up the explainer parameters, simply call the `transform` function on a `DataFrame` of observations to interpret the model behavior on these observations.

To see examples of model interpretability on Spark in action, take a look at these sample notebooks:

- [Tabular SHAP explainer](../notebooks/Interpretability%20-%20Tabular%20SHAP%20explainer.ipynb)
- [Image explainers](../notebooks/Interpretability%20-%20Image%20Explainers.ipynb)
- [Text explainers](../notebooks/Interpretability%20-%20Text%20Explainers.ipynb)

|                        | Tabular models              | Vector models             | Image models            | Text models           |
|------------------------|-----------------------------|---------------------------|-------------------------|-----------------------|
| LIME explainers        | [TabularLIME](#TabularLIME) | [VectorLIME](#VectorLIME) | [ImageLIME](#ImageLIME) | [TextLIME](#TextLIME) |
| Kernel SHAP explainers | [TabularSHAP](#TabularSHAP) | [VectorSHAP](#VectorSHAP) | [ImageSHAP](#ImageSHAP) | [TextSHAP](#TextSHAP) |

### Common local explainer params

All local explainers support the following params:

| Param            | Type          | Default       | Description                                                                                                                                                                                             |
|------------------|---------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| TargetCol        | `String`      | "probability" | The column name of the prediction target to explain (i.e. the response variable).  This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. |
| TargetClasses    | `Array[Int]`  | empty array   | The indices of the classes for multinomial classification models.                                                                                                                                       |
| TargetClassesCol | `String`      |               | The name of the column that specifies the indices of the classes for multinomial classification models.                                                                                                 |
| OutputCol        | `String`      |               | The name of the output column for interpretation results.                                                                                                                                               |
| Model            | `Transformer` |               | The model to be interpreted.                                                                                                                                                                            |

### Common LIME explainer params

All LIME based explainers ([TabularLIME](#TabularLIME), [VectorLIME](#VectorLIME), [ImageLIME](#ImageLIME), [TextLIME](#TextLIME)) support the following params:

| Param          | Type     | Default                         | Description                                               |
|----------------|----------|---------------------------------|-----------------------------------------------------------|
| regularization | `Double` | 0                               | Regularization param for the underlying lasso regression. |
| kernelWidth    | `Double` | sqrt(number of features) * 0.75 | Kernel width for the exponential kernel.                  |
| numSamples     | `Int`    | 1000                            | Number of samples to generate.                            |
| metricsCol     | `String` | "r2"                            | Column name for fitting metrics.                          |

### Common SHAP explainer params
  
All Kernel SHAP based explainers ([TabularSHAP](#TabularSHAP), [VectorSHAP](#VectorSHAP), [ImageSHAP](#ImageSHAP), [TextSHAP](#TextSHAP)) support the following params:

| Param      | Type     | Default                         | Description                                    |
|------------|----------|---------------------------------|------------------------------------------------|
| infWeight  | `Double` | 1E8                             | The double value to represent infinite weight. |
| numSamples | `Int`    | 2 * (number of features) + 2048 | Number of samples to generate.                 |
| metricsCol | `String` | "r2"                            | Column name for fitting metrics.               |

### Tabular model explainer params

All tabular model explainers ([TabularLIME](#TabularLIME), [TabularSHAP](#TabularSHAP)) support the following params:

| Param          | Type            | Default | Description                                                                                                  |
|----------------|-----------------|---------|--------------------------------------------------------------------------------------------------------------|
| InputCols      | `Array[String]` |         | The names of input columns to the black-box model.                                                           |
| backgroundData | `DataFrame`     |         | A dataframe containing background data. It must contain all the input columns needed by the black-box model. |

### Vector model explainer params

All vector model explainers ([VectorLIME](#VectorLIME), [VectorSHAP](#VectorSHAP)) support the following params:

| Param          | Type        | Default | Description                                                                                                    |
|----------------|-------------|---------|----------------------------------------------------------------------------------------------------------------|
| InputCol       | `String`    |         | The names of input vector column to the black-box model.                                                       |
| backgroundData | `DataFrame` |         | A dataframe containing background data. It must contain the input vector column needed by the black-box model. |

### Image model explainer params

All image model explainers ([ImageLIME](#ImageLIME), [ImageSHAP](#ImageSHAP)) support the following params:

| Param         | Type     | Default       | Description                                                       |
|---------------|----------|---------------|-------------------------------------------------------------------|
| InputCol      | `String` |               | The names of input image column to the black-box model.           |
| CellSize      | `Double` | 16            | Number that controls the size of the superpixels.                 |
| Modifier      | `Double` | 130           | Controls the trade-off spatial and color distance of superpixels. |
| SuperpixelCol | `String` | "superpixels" | The column holding the superpixel decompositions.                 |

### Text model explainer params

All text model explainers ([TextLIME](#TextLIME), [TextSHAP](#TextSHAP)) support the following params:

| Param     | Type     | Default  | Description                                            |
|-----------|----------|----------|--------------------------------------------------------|
| InputCol  | `String` |          | The names of input text column to the black-box model. |
| TokensCol | `String` | "tokens" | The column holding the text tokens.                    |

### `TabularLIME`

| Param               | Type            | Default     | Description                                                          |
|---------------------|-----------------|-------------|----------------------------------------------------------------------|
| CategoricalFeatures | `Array[String]` | empty array | The name of columns that should be treated as categorical variables. |

### `TabularSHAP`

No additional params are supported.

### `VectorLIME`

No additional params are supported.

### `VectorSHAP`

No additional params are supported.

### `ImageLIME`

| Param            | Type     | Default | Description                                             |
|------------------|----------|---------|---------------------------------------------------------|
| SamplingFraction | `Double` | 0.7     | The fraction of superpixels to keep on during sampling. |

### `ImageSHAP`

No additional params are supported.

### `TextLIME`

| Param            | Type     | Default | Description                                                   |
|------------------|----------|---------|---------------------------------------------------------------|
| SamplingFraction | `Double` | 0.7     | The fraction of tokens (for text) to keep on during sampling. |

### `TextSHAP`

No additional params are supported.

## Result interpretation

### LIME explainers

LIME explainers return an array of vectors, and each vector maps to a class being explained. Each component of the vector is the coefficient for the corresponding feature/superpixel/token from the local surrogate model.

- For categorical variables, superpixels, or tokens, the coefficient shows the average change in model outcome if this feature is unknown to the model, if the superpixcel is replaced with background color (black), or if the token is replaced with empty string.
- For numeric variables, the coefficient shows the change in model outcome if the feature value is incremented by 1 unit.

### SHAP explainers

SHAP explainers return an array of vectors, and each vector maps to a class being explained. Each vector starts with the base value, and each following component of the vector is the Shapley value for each feature/superpixel/token.

The base value and Shapley values are additive, and they should add up to the model output for the target observation.

#### Base value

- For tabular and vector models, the base value represents the mean outcome of the model for the background dataset.
- For image models, the base value represents the model outcome for a background (all black) image.
- For text models, the base value represents the model outcome for an empty string.
