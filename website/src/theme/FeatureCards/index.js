import React from "react";

import styles from "./styles.module.css";
import useBaseUrl from "@docusaurus/useBaseUrl";

const features = [
  {
    src: "/img/notebooks/cog_services_on_spark_2.svg",
    title: "The Cognitive Services on Spark",
    body: "Leverage the Microsoft Cognitive Services at unprecedented scales in your existing SparkML pipelines.",
    footer: "Read the Paper",
    burl: "https://arxiv.org/abs/1810.08744",
  },
  {
    src: "/img/notebooks/SparkServing3.svg",
    title: "Stress Free Serving",
    body: "Spark is well known for it's ability to switch between batch and streaming workloads by modifying a single line. \
      We push this concept even further and enable distributed web services with the same API as batch and streaming workloads.",
    footer: "Learn More",
    burl: "notebooks/Spark%20Serving/about",
  },
  {
    src: "/img/notebooks/decision_tree_recolor.png",
    title: "Lightning Fast Gradient Boosting",
    body: "MMLSpark adds GPU enabled gradient boosted machines from the popular framework LightGBM. \
    Users can mix and match frameworks in a single distributed environment and API.",
    footer: "Try an Example",
    burl: "notebooks/LightGBM/LightGBM%20-%20Overview",
  },
  {
    src: "/img/notebooks/vw-blue-dark-orange.svg",
    title: "Fast and Sparse Text Analytics",
    body: "Vowpal Wabbit on Spark enables new classes of workloads in scalable and performant text analytics",
    footer: "Try an Example",
    burl: "notebooks/Vowpal%20Wabbit/Vowpal%20Wabbit%20-%20Overview",
  },
  {
    src: "/img/notebooks/microservice_recolor.png",
    title: "Distributed Microservices",
    body: "MMLSpark provides powerful and idiomatic tools to communicate with any HTTP endpoint service using Spark. \
    Users can now use Spark as a elastic micro-service orchestrator.",
    footer: "Learn More",
    burl: "notebooks/HTTP/about",
  },
  {
    src: "/img/notebooks/LIME-1.svg",
    title: "Large Scale Model Interpretability",
    body: "Understand any image classifier with a distributed implementation of Local Interpretable Model Agnostic Explanations (LIME).",
    footer: "Try an Example",
    burl: "notebooks/Model%20Interpretation/ModelInterpretation%20-%20Snow%20Leopard%20Detection",
  },
  {
    src: "/img/notebooks/cntk-1.svg",
    title: "Scalable Deep Learning",
    body: "MMLSpark integrates the distributed computing framework Apache Spark with the flexible deep learning framework CNTK. \
    Enabling deep learning at unprecedented scales.",
    footer: "Read the Paper",
    burl: "https://arxiv.org/abs/1804.04031",
  },
  {
    src: "/img/multilingual.svg",
    title: "Broad Language Support",
    body: "MMLSpark's API spans Scala, Python, Java, R, .NET and C# so you can integrate with any ecosystem.",
    footer: "Try our PySpark Examples",
    burl: "notebooks/about",
  },
];

function FeatureCards() {

  return (
    features &&
    features.length && (
      <div className={styles.layout_grid_row}>
        {features.map((props, idx) => (
          <FeatureCard key={idx} {...props} />
        ))}
      </div>
    )
  );
}

function FeatureCard({ src, title, body, footer, burl }) {
  const srcUrl = useBaseUrl(src)
  return (
    <div class={styles.feature_card}>
      <div class={styles.card}>
        <div class={styles.card__image}>
          <img
            src={srcUrl}
            alt="Image alt text"
            title="Logo Title Text 1"
            height="200"
          />
        </div>
        <div class={styles.card__body}>
          <h4>{title}</h4>
          <small>{body}</small>
        </div>
        <div class={styles.card__footer}>
          <a class="button button--primary button--block" href={burl}>
            {footer}
          </a>
        </div>
      </div>
    </div>
  );
}

export default FeatureCards;
