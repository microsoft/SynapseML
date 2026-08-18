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
import clsx from "clsx";
import installArtifacts from "@site/src/installArtifacts";

const { repository, spark35, spark40, spark41 } = installArtifacts;

const snippets = [
  {
    label: "Cognitive Services",
    further:
      "docs/Explore%20Algorithms/AI%20Services/Overview#perform-sentiment-analysis-on-text",
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
    further: "docs/Explore%20Algorithms/Deep%20Learning/ONNX",
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
    label: "Responsible AI",
    further: "docs/Explore%20Algorithms/Responsible%20AI/Interpreting%20Model%20Predictions",
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
    further: "docs/Explore%20Algorithms/LightGBM/Overview",
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
  {
    label: "OpenCV",
    further:
      "docs/Explore%20Algorithms/OpenCV/Image%20Transformations",
    config: `from synapse.ml.opencv import *

image_df = (ImageTransformer()
    .setInputCol("images")
    .setOutputCol("transformed_images")
    .resize(224, True)
    .centerCrop(224, 224)
    .normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225], color_scale_factor = 1/255)
    .transform(input_df))`,
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
          SynapseML is Open Source and can be installed on supported Spark 3.5
          and Spark 4 infrastructure, including your local machine, Databricks,
          Synapse Analytics, and others.
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
      <div>{description}</div>
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
                  to={useBaseUrl("docs/Get%20Started/Install%20SynapseML")}
                >
                  Get Started
                </Link>
              </div>
            </div>
            <div className={classnames("col col--5")}>
              <img
                className={styles.heroImg}
                src={useBaseUrl("img/logo.svg")}
              />
            </div>
          </div>
        </div>
      </header>
      <main>
        <div className="container">
          <div className={clsx(styles.announcement, styles.announcementDark)}>
            <div className={styles.announcementInner}>
              Coming from{" "}
              <a href="https://mmlspark.blob.core.windows.net/website/index.html">
                MMLSpark
              </a>
              ? We have been renamed to SynapseML!
            </div>
          </div>
        </div>
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
                SynapseML&apos;s Python package supplies language wrappers; Spark
                must also load the JVM artifact matching its Scala binary
                version.{" "}
                <a href="https://github.com/microsoft/SynapseML">Open source</a>{" "}
                and cloud native.
              </p>
              <table>
                <thead>
                  <tr>
                    <th>Spark</th>
                    <th>Scala</th>
                    <th>Python baseline</th>
                    <th>Maven coordinate</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>3.5</td>
                    <td>{spark35.scalaBinaryVersion}</td>
                    <td>{spark35.pythonBaseline}</td>
                    <td><code>{spark35.coordinate}</code></td>
                  </tr>
                  <tr>
                    <td>4.0</td>
                    <td>{spark40.scalaBinaryVersion}</td>
                    <td>{spark40.pythonBaseline}</td>
                    <td><code>{spark40.coordinate}</code></td>
                  </tr>
                  <tr>
                    <td>4.1</td>
                    <td>{spark41.scalaBinaryVersion}</td>
                    <td>{spark41.pythonBaseline}</td>
                    <td><code>{spark41.coordinate}</code></td>
                  </tr>
                </tbody>
              </table>
              <Tabs
                defaultValue="Fabric"
                values={[
                  { label: "Synapse", value: "Synapse" },
                  { label: "Fabric", value: "Fabric" },
                  { label: "Spark Packages", value: "Spark Packages" },
                  { label: "Databricks", value: "Databricks" },
                  { label: "Docker", value: "Docker" },
                  { label: "Python", value: "Python" },
                  { label: "SBT", value: "SBT" },
                ]}
              >
                <TabItem value="Synapse">
                  <p>SynapseML can be installed on Synapse adding the following to the first cell of a notebook:</p>
                  For Spark3.5 pools:
                  <CodeSnippet
                    snippet={`%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.1.3",
      "spark.jars.repositories": "${repository}",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}`}
                    lang="bash"
                  ></CodeSnippet>
                  For Spark3.4 pools:
                  <CodeSnippet
                    snippet={`%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.15",
      "spark.jars.repositories": "${repository}",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}`}
                    lang="bash"
                  ></CodeSnippet>
                </TabItem>
                <TabItem value="Fabric">
                  <p>
                    SynapseML is preinstalled on Fabric. Before overriding it,
                    check the runtime&apos;s Spark and Scala versions. This
                    example selects the published Spark 4.1 / Scala 2.13
                    artifact:
                  </p>
                  <CodeSnippet
                    snippet={`%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "${spark41.coordinate}",
      "spark.jars.repositories": "${repository}",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.13,org.scalactic:scalactic_2.13,org.scalatest:scalatest_2.13,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}`}
                    lang="bash"
                  ></CodeSnippet>
                </TabItem>
                <TabItem value="Spark Packages">
                  SynapseML can be conveniently installed on existing Spark
                  clusters via the --packages option:
                  <CodeSnippet
                    snippet={`# Spark 4.1
pyspark --repositories "${repository}" --packages "${spark41.coordinate}"

# Spark 4.0
pyspark --repositories "${repository}" --packages "${spark40.coordinate}"

# Spark 3.5
pyspark --repositories "${repository}" --packages "${spark35.coordinate}"`}
                    lang="bash"
                  ></CodeSnippet>
                  This can be used in other Spark contexts too. For example, you
                  can use SynapseML in{" "}
                  <a href="https://github.com/Azure/aztk/">AZTK</a> by adding it
                  to the{" "}
                  <a href="https://github.com/Azure/aztk/wiki/PySpark-on-Azure-with-AZTK#optional-set-up-mmlspark">
                    .aztk/spark-defaults.conf file
                  </a>
                  .
                </TabItem>
                <TabItem value="Databricks">
                  <p>
                    To install SynapseML on the{" "}
                    <a href="http://community.cloud.databricks.com">
                      Databricks cloud
                    </a>
                    , create a new{" "}
                    <a href="https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages">
                      library from Maven coordinates
                    </a>{" "}
                    in your workspace.
                  </p>
                  <div>
                    <p>Choose the coordinate matching the cluster runtime:</p>
                    <p>Spark 4.1 / Scala 2.13:</p>
                    <CodeSnippet
                      snippet={spark41.coordinate}
                      lang="bash"
                    ></CodeSnippet>
                    <p>Spark 4.0 / Scala 2.13:</p>
                    <CodeSnippet
                      snippet={spark40.coordinate}
                      lang="bash"
                    ></CodeSnippet>
                    <p>Spark 3.5 / Scala 2.12:</p>
                    <CodeSnippet
                      snippet={spark35.coordinate}
                      lang="bash"
                    ></CodeSnippet>
                    <p>Use the following resolver:</p>
                    <CodeSnippet
                      snippet={repository}
                      lang="bash"
                    ></CodeSnippet>
                    <p>
                      Ensure this library is attached to your target cluster(s).
                    </p>
                  </div>
                  <p>
                    Restart the cluster after attaching the library so the JVM
                    artifact is available before importing <code>synapse.ml</code>.
                  </p>
                  You can use SynapseML in both your Scala and PySpark
                  notebooks. To get started with our example notebooks import
                  the following databricks archive:
                  <CodeSnippet
                    snippet={`https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv1.1.3.dbc`}
                    lang="bash"
                  ></CodeSnippet>
                </TabItem>
                <TabItem value="Docker">
                  The easiest way to evaluate SynapseML is via our pre-built
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
                  Install both the Python wrapper and the PySpark version
                  matching the selected JVM artifact.
                  <CodeSnippet
                    snippet={`# Spark 4.1 / Python ${spark41.pythonBaseline}
python -m pip install "${spark41.pythonPackage}" "pyspark${spark41.pysparkSpec}"

# Spark 4.0 / Python ${spark40.pythonBaseline}
python -m pip install "${spark40.pythonPackage}" "pyspark${spark40.pysparkSpec}"

# Spark 3.5 / Python ${spark35.pythonBaseline}
python -m pip install "${spark35.pythonPackage}" "pyspark${spark35.pysparkSpec}"`}
                    lang="bash"
                  ></CodeSnippet>
                  <CodeSnippet
                    snippet={`from pyspark.sql import SparkSession

# Spark 4.1; use "${spark40.coordinate}" for Spark 4.0 or
# "${spark35.coordinate}" for Spark 3.5.
coordinate = "${spark41.coordinate}"
spark = (
    SparkSession.builder.appName("MyApp")
    .config("spark.jars.packages", coordinate)
    .config("spark.jars.repositories", "${repository}")
    .getOrCreate()
)
import synapse.ml`}
                    lang="python"
                  ></CodeSnippet>
                </TabItem>
                <TabItem value="SBT">
                  If you are building a Spark application in Scala, add the
                  following lines to your build.sbt:
                  <CodeSnippet
                    snippet={`resolvers += "SynapseML" at "${repository}"

// Spark 4.1; use "${spark40.coordinate}" for Spark 4.0.
libraryDependencies +=
  "com.microsoft.azure" % "synapseml_2.13" % "${spark41.coordinate.split(":")[2]}"

// Spark 3.5:
// libraryDependencies +=
//   "com.microsoft.azure" % "synapseml_2.12" % "${spark35.coordinate.split(":")[2]}"`}
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
