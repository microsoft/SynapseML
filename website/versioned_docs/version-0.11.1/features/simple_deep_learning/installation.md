---
title: Installation Guidance
sidebar_label: Installation Guidance for Deep Vision Classification
---

:::note
This is a sample with databricks 10.4.x-gpu-ml-scala2.12 runtime
:::

## 1. Reinstall horovod using our prepared script

We build on top of torchvision, horovod and pytorch_lightning, so we need to reinstall horovod by building on specific versions of those packages.
Download our [horovod installation script](https://mmlspark.blob.core.windows.net/publicwasb/horovod_installation.sh) and upload
it to databricks dbfs.

Add the path of this script to `Init Scripts` section when configuring the spark cluster.
Restarting the cluster automatically installs horovod v0.25.0 with pytorch_lightning v1.5.0 and torchvision v0.12.0.

## 2. Install SynapseML Deep Learning Component

You could install the single synapseml-deep-learning wheel package to get the full functionality of deep vision classification.
Run the following command:
```powershell
pip install synapseml==0.11.1
```

An alternative is installing the SynapseML jar package in library management section, by adding:
```
Coordinate: com.microsoft.azure:synapseml_2.12:0.11.1
Repository: https://mmlspark.azureedge.net/maven
```
:::note
If you install the jar package, follow the first two cells of this [sample](./DeepLearning%20-%20Deep%20Vision%20Classification.md/#environment-setup----reinstall-horovod-based-on-new-version-of-pytorch)
to ensure horovod recognizes SynapseML.
:::

## 3. Try our sample notebook

You could follow the rest of this [sample](./DeepLearning%20-%20Deep%20Vision%20Classification.md) and have a try on your own dataset.

Supported models (`backbone` parameter for `DeepVisionClassifer`) should be string format of [Torchvision-supported models](https://github.com/pytorch/vision/blob/v0.12.0/torchvision/models/__init__.py);
You could also check by running `backbone in torchvision.models.__dict__`.
