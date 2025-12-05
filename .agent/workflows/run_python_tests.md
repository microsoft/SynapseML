---
description: How to run Python tests locally
---

To run Python tests locally, you need to generate them using sbt and then run them using pytest.

1.  **Generate the tests**:
    Run the following command to generate Python tests for a specific project (e.g., `core`, `cognitive`, `deepLearning`).
    ```bash
    sbt "<project>/pyTestgen"
    ```
    Example:
    ```bash
    sbt "core/pyTestgen"
    ```

2.  **Install Dependencies**:
    Ensure you have the necessary Python dependencies installed.
    ```bash
    pip install pytest pyspark==4.0.1
    ```

3.  **Configure `init_spark.py` (Crucial for Local Builds)**:
    The generated `init_spark.py` defaults to pulling `synapseml` from Maven. Since you are building locally, you must point it to your local jar.
    Edit `core/src/main/python/synapse/ml/core/init_spark.py` (or the generated version) to use `spark.jars` with the path to your built jar:
    ```python
    # Example modification
    jar_path = "/absolute/path/to/synapseml-core_2.13-<version>.jar"
    .config("spark.jars", jar_path)
    ```
    *Note: I have already updated the source to use Scala 2.13, but you may need to set the local jar path if the artifact is not published.*

    **Troubleshooting Missing Classes**:
    If you encounter `TypeError: 'JavaPackage' object is not callable`, it means classes are missing from the jar. Run:
    ```bash
    sbt "core/package"
    ```
    Then update `init_spark.py` to point to the newly built jar in `core/target/scala-2.13/`.

4.  **Run the tests**:
    Set `PYTHONPATH` to include the generated source and test directories, then run pytest.
    ```bash
    export PYTHONPATH=$PYTHONPATH:$(pwd)/core/target/scala-2.13/generated/src/python:$(pwd)/core/target/scala-2.13/generated/test/python
    pytest core/target/scala-2.13/generated/test/python
    ```

    To ignore broken tests:
    ```bash
    pytest core/target/scala-2.13/generated/test/python --ignore=core/target/scala-2.13/generated/test/python/synapsemltest/recommendation/test_ranking.py
    ```
