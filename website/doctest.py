import io
import os
import re
import sys


def add_python_helper_to_markdown(folder, md, version):
    replacement = """<!--
```python
import pyspark
import os
import json
from IPython.display import display
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "jupyter"
os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = "notebook"

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.13:{}")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {{}}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

import synapse.ml
```
-->

<!--pytest-codeblocks:cont-->""".format(
        version,
    )
    with io.open(os.path.join(folder, md), "r+", encoding="utf-8") as f:
        content = f.read()
        f.truncate(0)
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
    os.system(
        "pytest --codeblocks --junit-xml={}".format(
            os.path.join(cur_path, "target", "website-test-result.xml"),
        ),
    )


if __name__ == "__main__":
    main(str(sys.argv[1]))
