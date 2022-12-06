# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import pytest
import torch
from pyspark.sql import SparkSession
from pytorch_lightning import Trainer
from torch.utils.data import DataLoader, Dataset
from transformers import AutoTokenizer

from .conftest import _prepare_text_data
from .test_deep_vision_model import MyDummyCallback

from synapse.ml.dl import *


@pytest.mark.skip(reason="skip this as it takes too long")
def test_lit_deep_text_model():
    class TextDataset(Dataset):
        def __init__(self, data, tokenizer, max_token_len):
            super(TextDataset, self).__init__()
            self.data = data
            self.tokenizer = tokenizer
            self.max_token_len = max_token_len

        def __getitem__(self, index):
            text = self.data["Text"][index]
            label = self.data["label"][index]
            encoding = self.tokenizer(
                text,
                truncation=True,
                padding="max_length",
                max_length=self.max_token_len,
                return_tensors="pt",
            )
            return {
                "input_ids": encoding["input_ids"].flatten(),
                "attention_mask": encoding["attention_mask"].flatten(),
                "labels": torch.tensor([label], dtype=int),
            }

        def __len__(self):
            return len(self.data)

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    train_df, test_df = _prepare_text_data(spark)

    checkpoint = "bert-base-cased"
    tokenizer = AutoTokenizer.from_pretrained(checkpoint)
    max_token_len = 128

    train_loader = DataLoader(
        TextDataset(train_df.toPandas(), tokenizer, max_token_len),
        batch_size=16,
        shuffle=True,
        num_workers=0,
        pin_memory=True,
    )

    test_loader = DataLoader(
        TextDataset(test_df.toPandas(), tokenizer, max_token_len),
        batch_size=16,
        shuffle=True,
        num_workers=0,
        pin_memory=True,
    )

    epochs = 1
    model = LitDeepTextModel(
        checkpoint=checkpoint,
        additional_layers_to_train=10,
        num_labels=6,
        optimizer_name="adam",
        loss_name="cross_entropy",
        label_col="label",
        text_col="Text",
    )

    callbacks = [MyDummyCallback(epochs)]
    trainer = Trainer(callbacks=callbacks, max_epochs=epochs)
    trainer.fit(model, train_dataloaders=train_loader)
    trainer.test(model, dataloaders=test_loader)
