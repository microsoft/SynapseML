import React from "react";
import classnames from "classnames";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./videos.module.css";
import ReactPlayer from "react-player/youtube";

function Videos() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;
  return (
    <Layout
      title={`${siteConfig.title} Videos`}
      description="A collection of Benthos videos"
    >
      <header>
        <div className="container">
          <div className="row">
            <div className={classnames("col col--6 col--offset-1")}>
              <h1 className={styles.videosTitle}>SynapseML Videos</h1>
            </div>
          </div>
        </div>
      </header>
      <main>
        <div className="container margin-vert--lg">
          <div className="row margin-bottom--lg">
            <div className="col col--8 col--offset-2">
              <h1>Unsupervised Currency Detection</h1>
              <h2>Spark + AI Summit Keynote 2019</h2>
              <p>
                We use Bing on Spark, CNTK on Spark, Spark Serving, and ML Ops
                to help those with visual impairments work with currency.
              </p>
            </div>
            <ReactPlayer
              className={classnames("col col--8 col--offset-2")}
              url="https://www.youtube.com/watch?v=T_fs4C0aqD0&t=425s"
              controls="true"
            />
          </div>
          <div className="row margin-vert--lg">
            <div className="col col--8 col--offset-2">
              <h1>Unsupervised Fire Safety</h1>
              <h2>Spark + AI Summit Europe Keynote 2018</h2>
              <p>
                We use Bing on Spark, CNTK on Spark, and Spark serving to create
                a automated fire detection service for gas station safety. We
                then deploy this to an FPGA accelerated camera for Shell
                Industries.
              </p>
            </div>
            <ReactPlayer
              className={classnames("col col--8 col--offset-2")}
              url="https://www.youtube.com/watch?v=N3ozCZXeOeU&t=472s"
              controls="true"
            />
          </div>
          <div className="row margin-vert--lg">
            <div className="col col--8 col--offset-2">
              <h1>Predictive Maintenance with UAVs</h1>
              <h2>Spark + AI Summit 2018</h2>
              <p>
                We use CNTK on Spark to distribute a Faster RCNN object
                detection network and deploy it as a web service with MMLSpark
                Serving for use on Unmanned Aerial Vehicals (UAVs)
              </p>
              <a
                href="https://databricks.com/sparkaisummit/north-america/spark-summit-2018-keynotes#Intelligent-cloud"
                class={styles.watchNowButton}
              >
                Watch Now
              </a>
            </div>
          </div>
          <div className="row margin-vert--lg">
            <div className="col col--8 col--offset-2">
              <h1>Automated Snow Leopard Detection</h1>
              <p>
                We have partnered with the Snow Leopard Trust to create an
                intelligent snow leopard identification system. This project
                helped eliminate thousands of hours of searching through photos.
              </p>
              <div>
                <a
                  href="https://news.microsoft.com/transform/snow-leopard-selfies-ai-save-species/"
                  class={styles.watchNowButton}
                >
                  Read More on Microsoft Transform
                </a>
                <a
                  href="https://www.geekwire.com/2018/microsoft-says-ai-finally-ready-broader-use-help-solve-earths-environmental-woes/"
                  class={styles.watchNowButton}
                >
                  Read More on Geekwire
                </a>
              </div>
            </div>
          </div>
          <div className="row margin-vert--lg">
            <div className="col col--8 col--offset-2">
              <h1>Real-time Intelligent Analytics</h1>
              <h2>Microsoft Connect Keynote 2017</h2>
              <p>
                We use CNTK on Spark and deep transfer learning to create a
                real-time geospacial application for conservation biology in 5
                minutes
              </p>
              <a
                href="https://channel9.msdn.com/Events/Connect/2017/G102"
                class={styles.watchNowButton}
              >
                Watch Now
              </a>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}

export default Videos;
