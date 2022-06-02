# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import random
import shutil
import subprocess
import sys
import urllib
from os.path import join

import numpy as np
import pytest
import torch
import torchvision.transforms as transforms
from PIL import Image
from pytorch_lightning import Trainer
from pytorch_lightning.callbacks import Callback, ModelCheckpoint
from torch.utils.data import DataLoader, Dataset

sys.path.append(
    join(os.path.dirname(__file__), os.pardir, os.pardir, "synapse", "ml", "dl")
)

from LitDeepVisionModel import LitDeepVisionModel

# from synapse.ml.dl.LitDeepVisionModel import LitDeepVisionModel

IS_WINDOWS = os.name == "nt"
delimiter = "\\" if IS_WINDOWS else "/"
dataset_dir = (
    delimiter.join([os.getcwd(), "..", "..", "..", "..", "target"]) + delimiter
)


def _download_dataset():

    urllib.request.urlretrieve(
        "https://www.robots.ox.ac.uk/~vgg/data/flowers/17/17flowers.tgz",
        dataset_dir + "17flowers.tgz",
    )
    if os.path.exists(dataset_dir + "jpg"):
        shutil.rmtree(dataset_dir + "jpg")

    command = "tar -xzf {}17flowers.tgz -C {} \n".format(dataset_dir, dataset_dir)
    subprocess.run(command.split(), stdout=subprocess.PIPE)
    os.remove(dataset_dir + "17flowers.tgz")
    files = [
        join(dp, f)
        for dp, dn, filenames in os.walk(dataset_dir + "jpg")
        for f in filenames
        if os.path.splitext(f)[1] == ".jpg"
    ]
    assert len(files) == 1360
    np.random.shuffle(files)
    train_files, test_files = np.split(np.array(files), [int(len(files) * 0.75)])
    train_dir = dataset_dir + "jpg{}train".format(delimiter)
    test_dir = dataset_dir + "jpg{}test".format(delimiter)
    if not os.path.exists(train_dir):
        os.makedirs(train_dir)
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    for name in train_files:
        path, image = (
            delimiter.join(name.split(delimiter)[:-1]),
            name.split(delimiter)[-1],
        )
        shutil.move(name, delimiter.join([path, "train", image]))

    for name in test_files:
        path, image = (
            delimiter.join(name.split(delimiter)[:-1]),
            name.split(delimiter)[-1],
        )
        shutil.move(name, delimiter.join([path, "test", image]))


_download_dataset()


def test_lit_deep_vision_model():
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

    transform = transforms.Compose(
        [
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )

    train_loader = DataLoader(
        ImageDataset(
            dataset_dir + "jpg" + delimiter + "train",
            transform,
        ),
        batch_size=16,
        shuffle=True,
        num_workers=0,
        pin_memory=True,
    )

    test_loader = DataLoader(
        ImageDataset(
            dataset_dir + "jpg" + delimiter + "test",
            transform,
        ),
        batch_size=16,
        shuffle=True,
        num_workers=0,  # make sure this is 0 to avoid 'can't pickle local object Dataset' error
        pin_memory=True,
    )

    epochs = 10
    model = LitDeepVisionModel(
        backbone="resnet50",
        num_layers_to_train=1,
        num_classes=17,
        input_shapes=[[-1, 3, 224, 224]],
        optimizer_name="adam",
        loss_name="cross_entropy",
        label_cols=["label"],
        feature_cols=["image"],
    )

    class MyDummyCallback(Callback):
        def __init__(self):
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
            assert self.train_epcoh_end_counter <= epochs
            assert (
                self.train_epcoh_end_counter + self.validation_epoch_end_counter
                == self.epcoh_end_counter
            )

    callbacks = [MyDummyCallback(), ModelCheckpoint(dirpath="target/resnet50/")]
    trainer = Trainer(callbacks=callbacks, max_epochs=epochs)
    trainer.fit(model, train_dataloader=train_loader)
    trainer.test(model, dataloaders=test_loader)
