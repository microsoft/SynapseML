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

sentiment_df = (TextSentiment()
    .setTextCol("text")
    .setLocation("eastus")
    .setSubscriptionKey(key)
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setLanguageCol("language")
    .transform(input_df))`,
  },
  {
    label: "Deep Learning",
    further: "/docs/features/onnx/ONNX%20-%20Inference%20on%20Spark",
    config: `from synapse.ml.onnx import *

model_prediction_df = (ONNXModel()
    .setModelPayload(model_payload_ml)
    .setDeviceType("CPU")
    .setFeedDict({"input": "features"})
    .setFetchDict({"probability": "probabilities", "prediction": "label"})
    .setMiniBatchSize(64)
    .transform(input_df))`,
  },
  {
    label: "Model Interpretability",
    further: "/docs/features/model_interpretability/about",
    config: `from synapse.ml.explainers import *
    
interpretation_df = (TabularSHAP()
    .setInputCols(features)
    .setOutputCol("shapValues")
    .setTargetCol("probability")
    .setTargetClasses([1])
    .setNumSamples(5000)
    .setModel(model)
    .transform(input_df))`,
  },
  {
    label: "LightGBM",
    further: "/docs/features/lightgbm/about",
    config: `from synapse.ml.lightgbm import *
    
quantile_df = (LightGBMRegressor()
  .setApplication('quantile')
  .setAlpha(0.3)
  .setLearningRate(0.3)
  .setNumIterations(100)
  .setNumLeaves(31)
  .fit(train_df)
  .transform(test_df))`,
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
    imageUrl: "img/scalable.svg",
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
              <img className={styles.heroImg} src={useBaseUrl("img/logo.svg")} />
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
                    snippet={`spark-shell --packages com.microsoft.azure:synapseml:0.9.1
pyspark --packages com.microsoft.azure:synapseml:0.9.1
spark-submit --packages com.microsoft.azure:synapseml:0.9.1 MyApp.jar`}
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
                    in your workspace.
                  </p>
                  <p>
                    For the coordinates use:
                    <CodeSnippet
                      snippet={`com.microsoft.azure:synapseml:0.9.1`}
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
                    snippet={`https://mmlspark.blob.core.windows.net/dbcs/MMLSparkExamplesv0.9.1.dbc`}
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
        .config("spark.jars.packages", "com.microsoft.azure:synapseml:0.9.1")
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
libraryDependencies += "com.microsoft.azure" %% "synapseml" % "0.9.1"`}
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
