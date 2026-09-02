# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import io
import os
import re
import subprocess
import sys

COGNITIVE_RESOURCE_ID = (
    "/subscriptions/e342c2c0-f844-4b18-9208-52c8c234c30e/resourceGroups/"
    "marhamil-mmlspark/providers/Microsoft.CognitiveServices/accounts/mmlspark-cs"
)
COGNITIVE_SERVICE_NAME = "mmlspark-cs"
TRANSLATOR_ENDPOINT = (
    "https://mmlspark-cs.cognitiveservices.azure.com/translator/text/v3.0/"
)


def _use_aad_in_python_block(block, speech_sample):
    if speech_sample:
        block = block.replace(
            'cognitiveKey = os.environ.get("COGNITIVE_API_KEY", '
            'getSecret("cognitive-api-key"))',
            "cognitiveToken = getAccessToken()\n"
            f'cognitiveResourceId = "{COGNITIVE_RESOURCE_ID}"\n'
            'speechToken = f"aad#{cognitiveResourceId}#{cognitiveToken}"',
        )
        block = re.sub(
            r"(\.setSubscriptionKey\(cognitiveKey\)\n)(\s*)"
            r'(\.setLocation\("eastus"\))',
            r".setAADToken(speechToken)\n\2\3",
            block,
        )
        block = re.sub(
            r"(SpeechToTextSDK\(\)\n\s*)\.setAADToken\(speechToken\)\n(\s*)"
            r'(\.setLocation\("eastus"\))',
            r"\1.setAADToken(cognitiveToken)\n"
            r"\2.setCognitiveServiceResourceId(cognitiveResourceId)\n\2\3",
            block,
        )
        return block

    block = block.replace(
        'cognitiveKey = os.environ.get("COGNITIVE_API_KEY", '
        'getSecret("cognitive-api-key"))',
        "cognitiveToken = getAccessToken()",
    )
    block = block.replace(
        'textKey = os.environ.get("COGNITIVE_API_KEY", '
        'getSecret("cognitive-api-key"))',
        "cognitiveToken = getAccessToken()",
    )
    block = block.replace(
        'translatorKey = os.environ.get("TRANSLATOR_KEY", '
        'getSecret("translator-key"))',
        "cognitiveToken = getAccessToken()",
    )
    block = re.sub(
        r"\.setSubscriptionKey\((?:cognitiveKey|textKey)\)\n(\s*)"
        r'\.setLocation\("eastus"\)',
        rf'.setAADToken(cognitiveToken)\n\1.setCustomServiceName("{COGNITIVE_SERVICE_NAME}")',
        block,
    )
    block = re.sub(
        r"\.setSubscriptionKey\(translatorKey\)\n(\s*)" r'\.setLocation\("eastus"\)',
        r".setAADToken(cognitiveToken)\n"
        r'\1.setSubscriptionRegion("eastus")\n'
        rf'\1.setEndpoint("{TRANSLATOR_ENDPOINT}")',
        block,
    )
    return block


def use_aad_for_ci_samples(content, markdown_name):
    speech_sample = os.path.basename(markdown_name) == "_SpeechToText.md"
    return re.sub(
        r"```python\n.*?\n```",
        lambda match: _use_aad_in_python_block(match.group(0), speech_sample),
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
        f.truncate(0)
        content = use_aad_for_ci_samples(content, md)
        content = re.sub("<!--pytest-codeblocks:cont-->", replacement, content)
        f.seek(0, 0)
        f.write(content)
        f.close()


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
