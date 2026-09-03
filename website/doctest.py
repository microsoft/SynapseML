# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import io
import os
import re
import subprocess
import sys

COGNITIVE_LOCATION = "centralus"
TRANSLATOR_ENDPOINT = (
    "https://mmlspark-cs-central.cognitiveservices.azure.com/translator/text/v3.0/"
)
COGNITIVE_KEY_SAMPLES = {
    "_ComputerVision.md",
    "_Face.md",
    "_FormRecognizer.md",
    "_SpeechToText.md",
    "_TextAnalytics.md",
}


def _configure_python_block(block, markdown_name):
    if os.path.basename(markdown_name) in COGNITIVE_KEY_SAMPLES:
        return block.replace(
            'getSecret("cognitive-api-key")',
            'getSecret("cognitive-api-key-central")',
        ).replace(
            '.setLocation("eastus")',
            f'.setLocation("{COGNITIVE_LOCATION}")',
        )

    if os.path.basename(markdown_name) != "_Translator.md":
        return block

    block = block.replace(
        'translatorKey = os.environ.get("TRANSLATOR_KEY", '
        'getSecret("translator-key"))',
        "cognitiveToken = getAccessToken()",
    )
    block, auth_count = re.subn(
        r"\.setSubscriptionKey\(translatorKey\)",
        ".setAADToken(cognitiveToken)",
        block,
    )
    if auth_count:
        block = re.sub(
            r'(?m)^(\s*)\.setLocation\("eastus"\)',
            rf'\1.setSubscriptionRegion("{COGNITIVE_LOCATION}")\n'
            rf'\1.setEndpoint("{TRANSLATOR_ENDPOINT}")',
            block,
        )
    return block


def configure_ci_samples(content, markdown_name):
    return re.sub(
        r"```python\n.*?\n```",
        lambda match: _configure_python_block(match.group(0), markdown_name),
        content,
        flags=re.DOTALL,
    )


def add_python_helper_to_markdown(folder, md, version):
    replacement = """<!--
```python
import pyspark
import os
import json
import subprocess
from IPython.display import display
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "jupyter"
os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = "notebook"

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:{}")
        .config("spark.jars.repositories", "https://mmlspark.blob.core.windows.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {{}}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

def getAccessToken():
        result = subprocess.run(
            ["az", "account", "get-access-token", "--resource", "https://cognitiveservices.azure.com/"],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout)["accessToken"]

import synapse.ml
```
-->

<!--pytest-codeblocks:cont-->""".format(
        version,
    )
    with io.open(os.path.join(folder, md), "r+", encoding="utf-8") as f:
        content = f.read()
        content = configure_ci_samples(content, md)
        content = re.sub("<!--pytest-codeblocks:cont-->", replacement, content)
        f.seek(0)
        f.write(content)
        f.truncate()


def iterate_over_documentation(folder, version):
    cur_folders = [folder]
    while cur_folders:
        cur_dir = cur_folders.pop(0)
        for file in os.listdir(cur_dir):
            if os.path.isdir(os.path.join(cur_dir, file)):
                cur_folders.append(os.path.join(cur_dir, file))
            else:
                if file.startswith("_"):
                    add_python_helper_to_markdown(cur_dir, file, version)


def main(version):
    cur_path = os.getcwd()
    folder = os.path.join(cur_path, "docs", "Quick Examples")
    iterate_over_documentation(folder, version)
    os.chdir(folder)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--codeblocks",
            "--junit-xml={}".format(
                os.path.join(cur_path, "target", "website-test-result.xml"),
            ),
        ],
        check=True,
    )


if __name__ == "__main__":
    main(str(sys.argv[1]))
