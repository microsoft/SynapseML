# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
from setuptools import setup, find_packages
import codecs
import os.path

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


setup(
    name="mmlspark",
    version=get_version("mmlspark/__init__.py"),
    description="Microsoft ML for Spark",
    long_description="Microsoft ML for Apache Spark contains Microsoft's open source " +
                     "contributions to the Apache Spark ecosystem",
    license="MIT",
    packages=find_packages(),

    # Project's main homepage.
    url="https://github.com/Azure/mmlspark",
    # Author details
    author="Microsoft",
    author_email="mmlspark-support@microsoft.com",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Data Scientists",
        "Topic :: Software Development :: Datascience Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3"
    ],

    zip_safe=True,

    package_data={"mmlspark": ["../LICENSE.txt", "../README.txt"]}
)
