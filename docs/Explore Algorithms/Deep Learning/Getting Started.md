---
title: Getting Started
sidebar_label: Getting Started
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
pip install synapseml==1.1.3
```

The Python wheel supplies wrappers but does not install the JVM package. The
Maven example below targets Spark 3.5 / Scala 2.12:

```
Coordinate: com.microsoft.azure:synapseml_2.12:1.1.3
Repository: https://mmlspark.blob.core.windows.net/maven
```

For Spark 4.0 use
`com.microsoft.azure:synapseml_2.13:1.1.3-spark4.0`; for Spark 4.1 use
`com.microsoft.azure:synapseml_2.13:1.1.3-spark4.1`. See the
[installation guide](../../Get%20Started/Install%20SynapseML.md) for complete
copy-ready commands.
The historical Databricks 10.4.x runtime named above is an older Spark line; do
not combine it with the current Spark 3.5 artifact. If you retain that runtime,
select a compatible older SynapseML release instead.

:::note
If you install the jar package, follow the first two cells of this [sample](../Quickstart%20-%20Fine-tune%20a%20Vision%20Classifier#environment-setup----reinstall-horovod-based-on-new-version-of-pytorch)
to ensure horovod recognizes SynapseML.
:::

## 3. Try our sample notebook

You could follow the rest of this [sample](../Quickstart%20-%20Fine-Tune a Vision Classifier) and have a try on your own dataset.

Supported models (`backbone` parameter for `DeepVisionClassifer`) should be string format of [Torchvision-supported models](https://github.com/pytorch/vision/blob/v0.12.0/torchvision/models/__init__.py);
You could also check by running `backbone in torchvision.models.__dict__`.
