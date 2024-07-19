# Copyright (C) NVIDIA Corporation. All rights reserved.
# Licensed under the Apache License, See LICENSE in project root for information.

import os, json, subprocess, unittest
from synapsemltest.spark import *
from synapse.ml.hf import HuggingFaceSentenceEmbedder
from synapse.ml.nn import KNN
from pyspark.sql import SparkSession

class HuggingFaceSentenceTransformerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HuggingFaceSentenceTransformerTest, self).__init__(*args, **kwargs)
       
        self.miniLMSize = 384
        self.e5Size = 1024

        self.e5Transformer = (
              HuggingFaceSentenceEmbedder(
                modelName="intfloat/e5-large-v2", 
                inputCol="data", 
                outputCol="embeddings", 
                runtime="cpu")
        )

        self.miniLMTransformer = (
              HuggingFaceSentenceEmbedder(
                modelName="sentence-transformers/all-MiniLM-L6-v2",
                inputCol="data",
                outputCol="embeddings",
                runtime="cpu")
        )
        # construction of test dataframe
        # Attempt to use the Spark session if already initialized
        try:
            # If 'spark' is not defined, this will raise a NameError
            spark.sparkContext._jsc.sc()
        except NameError:
            # If 'spark' is not defined, initialize it
            spark = SparkSession.builder \
                .appName("Test App") \
                .getOrCreate()

        # self.sentenceDataFrame = spark.createDataFrame(
        #     [(1,"Happy"), (2,"Good"), (3,"Delicious"), (4,"Like it"),(5,"OK"), (6,"Disgusting"), (7,"Bad"), (8,"Don't like it"), (9,"Tastless"), (10,"Poor quality" )],
        #     ["id", "data"]
        # )
        self.sentenceDataFrame = spark.createDataFrame(
            [(1,"desserts"), (2,"disgusting")],
            ["id", "data"]
        ).cache()

    def test_e5_Embedding(self):
        transformed = self.e5Transformer.transform(self.sentenceDataFrame).cache()
        self._assert_embedding_df_size(self.sentenceDataFrame, transformed)
        self._assert_embedding_embedding_size(transformed, self.e5Size)

    def test_miniLM_Embedding(self):
        transformed = self.miniLMTransformer.transform(self.sentenceDataFrame).cache()
        self._assert_embedding_df_size(self.sentenceDataFrame, transformed)
        self._assert_embedding_embedding_size(transformed, self.miniLMSize)

    def _assert_embedding_embedding_size(self, transformed, expected_size):
        # Debugging to check the type
        collected_data = transformed.collect()

        for row in collected_data:
            embeddings_array = row['embeddings']
            size = len(embeddings_array)
            assert size == expected_size, f"Embedding size mismatch: expected {expected_size}, got {size}"

    def _assert_embedding_df_size(self, dataframe, transformed):
            num_rows = transformed.count()
            expected_num_rows = dataframe.count()
            assert num_rows == expected_num_rows, f"DataFrame size mismatch after transformation: expected {expected_num_rows}, got {num_rows}"        

if __name__ == "__main__":
    result = unittest.main()
