# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Import necessary libraries
import numpy as np
import torch
import pyspark.sql.functions as F
import tensorrt as trt
import logging
import warnings
import sys
import datetime
import pytz
from tqdm import tqdm, trange
from numpy import ndarray
from torch import Tensor
from typing import List, Union

import model_navigator as nav
from sentence_transformers import SentenceTransformer
from sentence_transformers.util import batch_to_device
from pyspark.ml.functions import predict_batch_udf
from faker import Faker

from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params
from pyspark.sql.functions import col, struct, rand
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
    FloatType,
)

class HuggingFaceSentenceEmbedder(Transformer, HasInputCol, HasOutputCol):
    """
    Custom transformer that extends PySpark's Transformer class to
    perform sentence embedding using a model with optional TensorRT acceleration.
    """

    # Define additional parameters
    runtime = Param(
        Params._dummy(),
        "runtime",
        "Specifies the runtime environment: cpu, cuda, or tensorrt",
    )
    batchSize = Param(Params._dummy(), "batchSize", "Batch size for embeddings", int)
    modelName = Param(Params._dummy(), "modelName", "Full Model Name parameter")

    class _SentenceTransformerNavigator(SentenceTransformer):
        """
        Inner class extending SentenceTransformer to override the encode method
        with additional functionality and optimizations (mainly to eliminate RecursiveErrors).
        """

        def encode(
            self,
            sentences: Union[str, List[str]],
            batch_size: int = 64,
            sentence_length: int = 512,
            show_progress_bar: bool = False,
            output_value: str = "sentence_embedding",
            convert_to_numpy: bool = True,
            convert_to_tensor: bool = False,
            device: str = None,
            normalize_embeddings: bool = False,
        ) -> Union[List[Tensor], ndarray, Tensor]:
            """
            Encode sentences into embeddings with optional configurations.
            """
            self.eval()
            show_progress_bar = (
                show_progress_bar if show_progress_bar is not None else True
            )
            convert_to_numpy = convert_to_numpy and not convert_to_tensor
            output_value = output_value or "sentence_embedding"

            # Handle input as a list of sentences
            input_was_string = isinstance(sentences, str) or not hasattr(
                sentences, "__len__"
            )
            if input_was_string:
                sentences = [sentences]

            # Determine the device to use for computation
            device = device or self._target_device
            self.to(device)

            # Initialize list for embeddings
            all_embeddings = []
            length_sorted_idx = np.argsort(
                [-self._text_length(sen) for sen in sentences]
            )
            sentences_sorted = [sentences[idx] for idx in length_sorted_idx]

            # Process sentences in batches
            for start_index in trange(
                0,
                len(sentences),
                batch_size,
                desc="Batches",
                disable=not show_progress_bar,
            ):
                sentences_batch = sentences_sorted[
                    start_index : start_index + batch_size
                ]
                features = self.tokenize(sentences_batch)
                features = batch_to_device(features, device)

                # Perform forward pass and gather embeddings
                with torch.no_grad():
                    out_features = self(features)

                    if output_value == "token_embeddings":
                        embeddings = []
                        for token_emb, attention in zip(
                            out_features[output_value], out_features["attention_mask"]
                        ):
                            last_mask_id = len(attention) - 1
                            while (
                                last_mask_id > 0 and attention[last_mask_id].item() == 0
                            ):
                                last_mask_id -= 1
                            embeddings.append(token_emb[0 : last_mask_id + 1])
                    elif output_value is None:
                        embeddings = []
                        for sent_idx in range(len(out_features["sentence_embedding"])):
                            row = {
                                name: out_features[name][sent_idx]
                                for name in out_features
                            }
                            embeddings.append(row)
                    else:
                        embeddings = out_features[output_value]
                        embeddings = embeddings.detach()
                        if normalize_embeddings:
                            embeddings = torch.nn.functional.normalize(
                                embeddings, p=2, dim=1
                            )
                        if convert_to_numpy:
                            embeddings = embeddings.cpu()

                    all_embeddings.extend(embeddings)

            # Restore original order of sentences
            all_embeddings = [
                all_embeddings[idx] for idx in np.argsort(length_sorted_idx)
            ]
            if convert_to_tensor:
                all_embeddings = torch.stack(all_embeddings)
            elif convert_to_numpy:
                all_embeddings = np.asarray([emb.numpy() for emb in all_embeddings])

            if input_was_string:
                all_embeddings = all_embeddings[0]

            return all_embeddings

    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        runtime=None,
        batchSize=16,
        modelName=None,
    ):
        """
        Initialize the HuggingFaceSentenceEmbedder with input/output columns and optional TRT flag.
        """
        super(HuggingFaceSentenceEmbedder, self).__init__()
        self._setDefault(
            runtime="cpu",
            modelName=modelName,
            batchSize=16,
        )
        self._set(
            inputCol=inputCol,
            outputCol=outputCol,
            runtime=runtime,
            batchSize=batchSize,
            modelName=modelName,
        )

    # Setter method for batchSize
    def setBatchSize(self, value):
        self._set(batchSize=value)
        return self

    # Getter method for batchSize
    def getBatchSize(self):
        return self.getOrDefault(self.batchSize)

    # Sets the runtime environment for the model.
    # Supported values: 'cpu', 'cuda', 'tensorrt'
    def setRuntime(self, value):
        """
        Sets the runtime environment for the model.
        Supported values: 'cpu', 'cuda', 'tensorrt'
        """
        if value not in ["cpu", "cuda", "tensorrt"]:
            raise ValueError(
                "Invalid runtime specified. Choose from 'cpu', 'cuda', 'tensorrt'"
            )
        self.setOrDefault(self.runtime, value)

    def getRuntime(self):
        return self.getOrDefault(self.runtime)

    # Setter method for modelName
    def setModelName(self, value):
        self._set(modelName=value)
        return self

    # Getter method for modelName
    def getModelName(self):
        return self.getOrDefault(self.modelName)

    # Optimize the model using Model Navigator with TensorRT configuration.
    def _optimize(self, model):
        conf = nav.OptimizeConfig(
            target_formats=(nav.Format.TENSORRT,),
            runners=("TensorRT",),
            optimization_profile=nav.OptimizationProfile(max_batch_size=64),
            custom_configs=[
                nav.TorchConfig(autocast=True),
                nav.TorchScriptConfig(autocast=True),
                nav.TensorRTConfig(
                    precision=(nav.TensorRTPrecision.FP16,),
                    onnx_parser_flags=[trt.OnnxParserFlag.NATIVE_INSTANCENORM.value],
                ),
            ],
        )

        def _gen_size_chunk():
            """
            Generate chunks of different batch sizes and sentence lengths.
            """
            for batch_size in [64]:
                for sentence_length in [20, 300, 512]:
                    yield (batch_size, sentence_length)

        def _get_dataloader(repeat_times: int = 2):
            """
            Create a data loader with synthetic data using Faker.
            """
            faker = Faker()
            i = 0
            for batch_size, chunk_size in _gen_size_chunk():
                for _ in range(repeat_times):
                    yield (
                        i,
                        (
                            [
                                " ".join(faker.words(chunk_size))
                                for _ in range(batch_size)
                            ],
                            {"show_progress_bar": False},
                        ),
                    )
                    i += 1

        total_batches = len(list(_gen_size_chunk()))
        func = lambda x, **kwargs: self._SentenceTransformerNavigator._encode(
            model, x, **kwargs
        )
        nav.optimize(
            func, dataloader=tqdm(_get_dataloader(), total=total_batches), config=conf
        )

    def _predict_batch_fn(self):
        """
        Create and return a function for batch prediction.
        """
        runtime = self.getRuntime()
        if "model" not in globals():
            global model
            modelName = self.getModelName()
            if runtime == "tensorrt":
                moduleName = modelName.split("/")[1]
                model = self._SentenceTransformerNavigator(modelName).eval()
                model = nav.Module(model, name=moduleName)
                try:
                    nav.load_optimized()
                except Exception:
                    self._optimize(model)
                    nav.load_optimized()
            else:
                model = SentenceTransformer(modelName).eval()

        def predict(inputs):
            """
            Predict method to encode inputs using the model.
            """
            with torch.no_grad():
                output = model.encode(
                    inputs.tolist(), convert_to_tensor=False, show_progress_bar=False
                )

            return output

        return predict

    # Method to apply the transformation to the dataset
    def _transform(self, dataset, spark):
        """
        Apply the transformation to the input dataset.
        """
        input_col = self.getInputCol()
        output_col = self.getOutputCol()

        encode = predict_batch_udf(
            self._predict_batch_fn,
            return_type=ArrayType(FloatType()),
            batch_size=self.getBatchSize(),
        )
        return dataset.withColumn(output_col, encode(input_col))

    def transform(self, dataset, spark=None):
        """
        Public method to transform the dataset.
        """
        return self._transform(dataset, spark)

    def copy(self, extra=None):
        """
        Create a copy of the transformer.
        """
        return self._defaultCopy(extra)

# Example usage:
# data = input data frame
# transformer = EmbeddingTransformer(inputCol="combined", outputCol="embeddings")
# result = transformer.transform(data)
# result.show()