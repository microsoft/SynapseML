# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from setuptools import find_namespace_packages, setup
from setuptools.command.install import install
from importlib.machinery import SourceFileLoader
import os
import subprocess

MINIMUM_SUPPORTED_PYTHON_VERSION = "3.8"

version = (
    SourceFileLoader("synapseml.dl.version", os.path.join(os.getcwd(), "version.py"))
    .load_module()
    .VERSION
)

pyspark_require_list = [
    'pyspark>=3.2.0;python_version>="{}"'.format(MINIMUM_SUPPORTED_PYTHON_VERSION)
]
spark_require_list = [
    "numpy",
    "petastorm>=0.11.0",
    "pyarrow>=0.15.0",
    "fsspec>=2021.07.0",
]  ## TO BE VERIFY
dl_require_list = [
    "cmake",
    "torch>=1.11.0",
    "pytorch_lightning>=1.5.0,<1.5.10",
]

dl_extra_list = ["torchvision>=0.12.0", "horovod==0.25.0"]


class PreInstallCommand(install):
    """Pre-installation for installation mode."""

    def run(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        script_path = os.path.join(dir_path, "horovod_installation.sh")
        print("Installing horovod ...")
        subprocess.call(script_path)
        install.run(self)


setup(
    name="synapseml-dl",
    version=version,
    description="Synapse Machine Learning for Deep Learning",
    long_description="SynapseML contains Microsoft's open source contributions to the Apache Spark ecosystem",
    license="MIT",
    packages=find_namespace_packages(where="src", include=["synapse.ml.*"]),
    package_dir={"": "src"},
    package_data={"synapseml": ["../LICENSE.txt", "../README.md"]},
    # cmdclass={'install': PreInstallCommand},
    install_requires=pyspark_require_list + spark_require_list + dl_require_list,
    extras_require={"extras": dl_extra_list},
    zip_safe=True,
    author="Microsoft",
    author_email="synapseml-support@microsoft.com",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
    ],
    url="https://github.com/Microsoft/SynapseML",
    python_requires=f">={MINIMUM_SUPPORTED_PYTHON_VERSION}",
    project_urls={
        "Website": "https://microsoft.github.io/SynapseML/",
        "Documentation": "https://mmlspark.blob.core.windows.net/docs/{}/pyspark/index.html".format(
            version
        ),
        "Source Code": "https://github.com/Microsoft/SynapseML",
    },
)
