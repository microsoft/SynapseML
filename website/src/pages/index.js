import React from "react";
import classnames from "classnames";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./index.module.css";
import CodeSnippet from "@site/src/theme/CodeSnippet";
import SampleSnippet from "@site/src/theme/SampleSnippet";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

const snippets = [
  {
    label: "Text Analytics",
    further:
      "/docs/features/CognitiveServices%20-%20Overview#text-analytics-sample",
    config: `from synapse.ml.cognitive import *
from pyspark.sql.functions import col
import os

# A general Cognitive Services key for Text Analytics and Computer Vision (or use separate keys that belong to each service)
service_key = os.environ["TEXT_API_KEY"]

# Create a dataframe that's tied to it's column names
df = spark.createDataFrame([
  ("I am so happy today, its sunny!", "en-US"),
  ("I am frustrated by this rush hour traffic", "en-US"),
  ("The cognitive services on spark aint bad", "en-US"),
], ["text", "language"])

# Run the Text Analytics service with options
sentiment = (TextSentiment()
    .setTextCol("text")
    .setLocation("eastus")
    .setSubscriptionKey(service_key)
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setLanguageCol("language"))

# Show the results of your text query in a table format
display(sentiment.transform(df).select("text", col("sentiment")[0].getItem("sentiment").alias("sentiment")))`,
  },
  {
    label: "Deep Learning",
    further: "/docs/features/onnx/ONNX%20-%20Inference%20on%20Spark",
    config: `# Load the ONNX payload into an ONNXModel, and inspect the model inputs and outputs.
from synapse.ml.onnx import ONNXModel

onnx_ml = ONNXModel().setModelPayload(model_payload_ml)

print("Model inputs:" + str(onnx_ml.getModelInputs()))
print("Model outputs:" + str(onnx_ml.getModelOutputs()))

# Map the model input to the input dataframe's column name (FeedDict), and map the output dataframe's column names to the model outputs (FetchDict).
onnx_ml = (
  onnx_ml
    .setDeviceType("CPU")
    .setFeedDict({"input": "features"})
    .setFetchDict({"probability": "probabilities", "prediction": "label"})
    .setMiniBatchSize(5000)
)

# Create some testing data and transform the data through the ONNX model.
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import numpy as np

n = 1000 * 1000
m = 95
test = np.random.rand(n, m)
testPdf = pd.DataFrame(test)
cols = list(map(str, testPdf.columns))
testDf = spark.createDataFrame(testPdf)
testDf = testDf.union(testDf).repartition(200)
testDf = VectorAssembler().setInputCols(cols).setOutputCol("features").transform(testDf).drop(*cols).cache()

display(onnx_ml.transform(testDf))
    `,
  },
  {
    label: "Model Interpretability",
    further: "/docs/features/model_interpretability/about",
    config: `from synapse.ml.explainers import *
import urllib.request

# We download an image for interpretation.
test_image_url = (
  "https://mmlspark.blob.core.windows.net/publicwasb/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"
)
with urllib.request.urlopen(test_image_url) as url:
  barr = url.read()

# Create a dataframe from the downloaded image, and use ResNet50 model to infer the image.
image_df = spark.createDataFrame([(bytearray(barr),)], ["image"])
network = ModelDownloader(spark, "dbfs:/Models/").downloadByName("ResNet50")
model = ImageFeaturizer(inputCol="image", outputCol="probability", cutOutputLayers=0).setModel(network)

predicted = (
  model.transform(image_df)
  .withColumn("top2pred", arg_top(col("probability"), lit(2)))
  .withColumn("top2prob", vec_slice(col("probability"), col("top2pred")))
)

# Use the LIME image explainer to explain the model's top 2 classes' probabilities.
lime = (
  ImageLIME()
  .setModel(model)
  .setOutputCol("weights")
  .setInputCol("image")
  .setCellSize(50.0)
  .setModifier(20.0)
  .setNumSamples(500)
  .setMetricsCol("r2")
  .setTargetCol("probability")
  .setTargetClassesCol("top2pred")
  .setSamplingFraction(0.7)
)

lime_result = (
  lime.transform(predicted)
  .withColumn("weights_piano", col("weights").getItem(0))
  .withColumn("weights_cello", col("weights").getItem(1))
  .withColumn("r2_piano", vec_access("r2", lit(0)))
  .withColumn("r2_cello", vec_access("r2", lit(1)))
  .cache()
)

display(lime_result.select(col("weights_piano"), col("r2_piano"), col("weights_cello"), col("r2_cello")))
    `,
  },
  {
    label: "LightGBM",
    further: "/docs/features/lightgbm/about",
    config: `from synapse.ml.lightgbm import LightGBMRegressor
model = LightGBMRegressor(application='quantile',
                          alpha=0.3,
                          learningRate=0.3,
                          numIterations=100,
                          numLeaves=31).fit(train)`,
  },
];

const features = [
  {
    title: "Simple",
    imageUrl: "img/simple.svg",
    description: (
      <>
        <p>
          Quickly create, train, and use distributed machine learning tools in
          only a few lines of code.
        </p>
      </>
    ),
  },
  {
    title: "Scalable",
    imageUrl: "img/scalable3.svg",
    description: (
      <>
        <p>
          Scale ML workloads to hundreds of machines on your{" "}
          <a href="https://spark.apache.org/">Apache Spark</a> cluster.
        </p>
      </>
    ),
  },
  {
    title: "Multilingual",
    imageUrl: "img/multilingual.svg",
    description: (
      <>
        <p>
          Use SynapseML from any Spark compatible language including Python,
          Scala, R, Java, .NET and C#.
        </p>
      </>
    ),
  },
  {
    title: "Open",
    imageUrl: "img/open_source.svg",
    description: (
      <>
        <p>
          SynapseML is Open Source and can be installed and used on any Spark 3
          infrastructure including your local machine, Databricks, Synapse
          Analytics, and others.
        </p>
      </>
    ),
  },
];

function Feature({ imageUrl, title, description }) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={classnames("col col--6", styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img
            className={classnames("padding-vert--md", styles.featureImage)}
            src={imgUrl}
            alt={title}
          />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

function Home() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Simple and Distributed Machine Learning"
      keywords={["SynapseML", "Machine Learning"]}
    >
      <header className={classnames("hero", styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={classnames("col col--5 col--offset-1")}>
              <h1 className="hero__title">{siteConfig.title}</h1>
              <p className="hero__subtitle">{siteConfig.tagline}</p>
              <div className={styles.buttons}>
                <Link
                  className={classnames(
                    "button button--outline button--primary button--lg",
                    styles.getStarted
                  )}
                  to={useBaseUrl("docs/getting_started/installation")}
                >
                  Get Started
                </Link>
              </div>
            </div>
            <div className={classnames("col col--5")}>
              <img className={styles.heroImg} src="img/logo.svg" />
            </div>
          </div>
        </div>
      </header>
      <main>
        <div className="container">
          <div className="row">
            <div className={classnames("col col--12")}>
              {snippets && snippets.length && (
                <section className={styles.configSnippets}>
                  <Tabs
                    defaultValue={snippets[0].label}
                    values={snippets.map((props, idx) => {
                      return { label: props.label, value: props.label };
                    })}
                  >
                    {snippets.map((props, idx) => (
                      <TabItem key={idx} value={props.label}>
                        <SampleSnippet
                          className={styles.configSnippet}
                          {...props}
                        ></SampleSnippet>
                      </TabItem>
                    ))}
                  </Tabs>
                </section>
              )}
            </div>
          </div>
        </div>
        {features && features.length && (
          <section className={styles.features}>
            <div className="container margin-vert--md">
              <div className="row">
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </section>
        )}
        <div className="container">
          <div className="row">
            <div className={classnames(`${styles.pitch} col`)}>
              <h2>Installation</h2>
              <p>
                Written in Scala, and support multiple languages.{" "}
                <a href="https://github.com/microsoft/SynapseML">Open source</a>{" "}
                and cloud native.
              </p>
              <p>
                Note: SynpaseML is built-in for <a href="https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-3-runtime">Azure Synapse.</a>
              </p>
              <Tabs
                defaultValue="Spark Packages"
                values={[
                  { label: "Spark Packages", value: "Spark Packages" },
                  { label: "Databricks", value: "Databricks" },
                  { label: "Docker", value: "Docker" },
                  { label: "Python", value: "Python" },
                  { label: "SBT", value: "SBT" },
                ]}
              >
                <TabItem value="Spark Packages">
                  MMLSpark can be conveniently installed on existing Spark
                  clusters via the --packages option, examples:
                  <CodeSnippet
                    snippet={`spark-shell --packages com.microsoft.azure:synapseml:0.9.0
pyspark --packages com.microsoft.azure:synapseml:0.9.0
spark-submit --packages com.microsoft.azure:synapseml:0.9.0 MyApp.jar`}
                    lang="bash"
                  ></CodeSnippet>
                  This can be used in other Spark contexts too. For example, you
                  can use MMLSpark in{" "}
                  <a href="https://github.com/Azure/aztk/">AZTK</a> by adding it
                  to the{" "}
                  <a href="https://github.com/Azure/aztk/wiki/PySpark-on-Azure-with-AZTK#optional-set-up-mmlspark">
                    .aztk/spark-defaults.conf file
                  </a>
                  .
                </TabItem>
                <TabItem value="Databricks">
                  <p>
                    To install MMLSpark on the{" "}
                    <a href="http://community.cloud.databricks.com">
                      Databricks cloud
                    </a>
                    , create a new{" "}
                    <a href="https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages">
                      library from Maven coordinates
                    </a>{" "}
                    in your workspace.
                  </p>
                  <p>
                    For the coordinates use:
                    <CodeSnippet
                      snippet={`com.microsoft.azure:synapseml:0.9.0`}
                      lang="bash"
                    ></CodeSnippet>
                    with the resolver:
                    <CodeSnippet
                      snippet={`https://mmlspark.azureedge.net/maven`}
                      lang="bash"
                    ></CodeSnippet>
                    Ensure this library is attached to your target cluster(s).
                  </p>
                  <p>
                    Finally, ensure that your Spark cluster has at least Spark
                    2.4 and Scala 2.11.
                  </p>
                  You can use MMLSpark in both your Scala and PySpark notebooks.
                  To get started with our example notebooks import the following
                  databricks archive:
                  <CodeSnippet
                    snippet={`https://mmlspark.blob.core.windows.net/dbcs/MMLSparkExamplesv0.9.0.dbc`}
                    lang="bash"
                  ></CodeSnippet>
                </TabItem>
                <TabItem value="Docker">
                  The easiest way to evaluate MMLSpark is via our pre-built
                  Docker container. To do so, run the following command:
                  <CodeSnippet
                    snippet={`docker run -it -p 8888:8888 -e ACCEPT_EULA=yes mcr.microsoft.com/mmlspark/release`}
                    lang="bash"
                  ></CodeSnippet>
                  <p>
                    Navigate to{" "}
                    <a href="http://localhost:8888">http://localhost:8888</a> in
                    your web browser to run the sample notebooks. See the{" "}
                    <a href="https://github.com/microsoft/SynapseML/blob/master/docs/docker.md">
                      documentation
                    </a>{" "}
                    for more on Docker use.
                  </p>
                  To read the EULA for using the docker image, run
                  <CodeSnippet
                    snippet={`docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release eula`}
                    lang="bash"
                  ></CodeSnippet>
                </TabItem>
                <TabItem value="Python">
                  To try out MMLSpark on a Python (or Conda) installation you
                  can get Spark installed via pip with
                  <CodeSnippet
                    snippet={`pip install pyspark`}
                    lang="bash"
                  ></CodeSnippet>
                  You can then use pyspark as in the above example, or from
                  python:
                  <CodeSnippet
                    snippet={`import pyspark
spark = pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.azure:synapseml:0.9.0")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate()
import synapse.ml`}
                    lang="python"
                  ></CodeSnippet>
                </TabItem>
                <TabItem value="SBT">
                  If you are building a Spark application in Scala, add the
                  following lines to your build.sbt:
                  <CodeSnippet
                    snippet={`resolvers += "SynapseML" at "https://mmlspark.azureedge.net/maven"
libraryDependencies += "com.microsoft.azure" %% "synapseml" % "0.9.0"`}
                    lang="jsx"
                  ></CodeSnippet>
                </TabItem>
              </Tabs>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}

export default Home;
