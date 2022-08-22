# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import random
from os.path import join

import numpy as np
import torch
from PIL import Image
from pytorch_lightning import Trainer
from pytorch_lightning.callbacks import Callback
from torch.utils.data import DataLoader, Dataset

from synapse.ml.dl import *


class MyDummyCallback(Callback):
    def __init__(self, epochs=10):
        self.epochs = epochs
        self.epcoh_end_counter = 0
        self.train_epcoh_end_counter = 0
        self.validation_epoch_end_counter = 0

    def on_init_start(self, trainer):
        print("Starting to init trainer!")

    def on_init_end(self, trainer):
        print("Trainer is initialized.")

    def on_epoch_end(self, trainer, model):
        print("A epoch ended.")
        self.epcoh_end_counter += 1

    def on_train_epoch_end(self, trainer, model, unused=None):
        print("A train epoch ended.")
        self.train_epcoh_end_counter += 1

    def on_validation_epoch_end(self, trainer, model, unused=None):
        print("A val epoch ended.")
        self.validation_epoch_end_counter += 1

    def on_train_end(self, trainer, model):
        print(
            "Training ends:"
            f"epcoh_end_counter={self.epcoh_end_counter}, "
            f"train_epcoh_end_counter={self.train_epcoh_end_counter}, "
            f"validation_epoch_end_counter={self.validation_epoch_end_counter} \n"
        )
        assert self.train_epcoh_end_counter <= self.epochs
        assert (
            self.train_epcoh_end_counter + self.validation_epoch_end_counter
            == self.epcoh_end_counter
        )


def test_lit_deep_vision_model(transform, get_data_path):
    seed = np.random.randint(2147483647)
    random.seed(seed)
    torch.manual_seed(seed)

    class ImageDataset(Dataset):
        def __init__(self, root, transform):
            super(ImageDataset, self).__init__()
            self.root = join(root)
            self.transform = transform
            self.images = [f for f in os.listdir(self.root) if f.split(".")[1] == "jpg"]

        def __getitem__(self, index):
            image = Image.open(join(self.root, self.images[index])).convert("RGB")
            image = self.transform(image)
            label = int(self.images[index].split(".")[0].split("_")[1]) // 81
            return {"image": image, "label": label}

        def __len__(self):
            return len(self.images)

    train_folder, test_folder = get_data_path

    train_loader = DataLoader(
        ImageDataset(
            train_folder,
            transform,
        ),
        batch_size=16,
        shuffle=True,
        num_workers=0,
        pin_memory=True,
    )

    test_loader = DataLoader(
        ImageDataset(
            test_folder,
            transform,
        ),
        batch_size=16,
        shuffle=True,
        num_workers=0,  # make sure this is 0 to avoid 'can't pickle local object Dataset' error
        pin_memory=True,
    )

    epochs = 2
    model = LitDeepVisionModel(
        backbone="resnet50",
        additional_layers_to_train=1,
        num_classes=17,
        input_shape=[-1, 3, 224, 224],
        optimizer_name="adam",
        loss_name="cross_entropy",
        label_col="label",
        image_col="image",
    )

    callbacks = [MyDummyCallback(epochs)]
    trainer = Trainer(callbacks=callbacks, max_epochs=epochs)
    trainer.fit(model, train_dataloaders=train_loader)
    trainer.test(model, dataloaders=test_loader)
