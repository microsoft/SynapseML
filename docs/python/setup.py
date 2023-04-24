# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
from setuptools import setup, find_packages

setup(
    name="documentprojection",
    version=0.1,
    description="Synapse Machine Learning Documentation Pipeline",
    long_description="SynapseML contains Microsoft's open source "
    + "contributions to the Apache Spark ecosystem",
    license="MIT",
    packages=find_packages(),
    url="https://github.com/Microsoft/SynapseML",
    author="Microsoft",
    author_email="synapseml-support@microsoft.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Data Scientists",
        "Topic :: Software Development :: Datascience Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
    ],
    zip_safe=True,
    package_data={"documentprojection": ["../LICENSE.txt", "../README.txt"]},
    python_requires=">=3.10",
    install_requires=["nbformat", "nbconvert", "pathlib", "argparse"],
)
