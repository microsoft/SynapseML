import React from 'react';
import classnames from 'classnames';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './index.module.css';
import CodeSnippet from "@site/src/theme/CodeSnippet";
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

const installs = [
  {
    label: 'Curl',
    snippet: `# Install
curl -Lsf https://sh.benthos.dev | bash

# Make a config
benthos create nats/protobuf/aws_sqs > ./config.yaml

# Run
benthos -c ./config.yaml`
  },
  {
    label: 'Homebrew',
    snippet: `# Install
brew install benthos

# Make a config
benthos create nats/protobuf/aws_sqs > ./config.yaml

# Run
benthos -c ./config.yaml`
  },
  {
    label: 'Docker',
    snippet: `# Pull
docker pull jeffail/benthos

# Make a config
docker run --rm jeffail/benthos create nats/protobuf/aws_sqs > ./config.yaml

# Run
docker run --rm -v $(pwd)/config.yaml:/benthos.yaml jeffail/benthos`
  },
]

const snippets = [
  {
    label: 'Mapping',
    further: '/docs/guides/bloblang/about',
    config: `input:
  gcp_pubsub:
    project: foo
    subscription: bar

pipeline:
  processors:
    - bloblang: |
        root.message = this
        root.meta.link_count = this.links.length()
        root.user.age = this.user.age.number()

output:
  redis_streams:
    url: tcp://TODO:6379
    stream: baz
    max_in_flight: 20`,
  },
  {
    label: 'Multiplexing',
    further: '/docs/components/outputs/about#multiplexing-outputs',
    config: `input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

output:
  switch:
    cases:
      - check: doc.tags.contains("AWS")
        output:
          aws_sqs:
            url: https://sqs.us-west-2.amazonaws.com/TODO/TODO
            max_in_flight: 20

      - output:
          redis_pubsub:
            url: tcp://TODO:6379
            channel: baz
            max_in_flight: 20`,
  },
  {
    label: 'Enrichments',
    further: '/cookbooks/enrichments',
    config: `input:
  mqtt:
    urls: [ tcp://TODO:1883 ]
    topics: [ foo ]

pipeline:
  processors:
    - branch:
        request_map: |
          root.id = this.doc.id
          root.content = this.doc.body
        processors:
          - aws_lambda:
              function: sentiment_analysis
        result_map: root.results.sentiment = this

output:
  aws_s3:
    bucket: TODO
    path: '\${! meta("partition") }/\${! timestamp_unix_nano() }.tar.gz'
    batching:
      count: 100
      period: 10s
      processors:
        - archive:
            format: tar
        - compress:
            algorithm: gzip`,
  },
];

function Snippet({label, config}) {
  return (
    <CodeSnippet className={styles.configSnippet} snippet={config}></CodeSnippet>
  );
}

const features = [
  {
    title: 'Simple',
    imageUrl: 'img/simple.svg',
    description: (
      <>
        <p>
          Quickly create, train, and use distributed machine learning tools in only a few lines of code.   
        </p>
      </>
    ),
  },
  {
    title: 'Scalable',
    imageUrl: 'img/scalable2.svg',
    description: (
      <>
        <p>
         Scale ML workloads to hundreds of machines on your <a href="https://spark.apache.org/">Apache Spark</a> cluster.
        </p>
      </>
    ),
  },
  {
    title: 'Multilingual',
    imageUrl: 'img/multilingual.svg',
    description: (
      <>
        <p>
        Use SynapseML from any Spark compatible language including Python, Scala, R, Java, .NET and C#.
        </p>
      </>
    ),
  },
  {
    title: 'Open',
    imageUrl: 'img/Blobextended.svg',
    description: (
      <>
        <p>
          SynapseML is Open Source and can be installed and used on any Spark 3 infrastructure including your local machine, Databricks, Synapse Analytics, and others.
        </p>
      </>
    ),
  },
];

function Feature({imageUrl, title, description}) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={classnames('col col--6', styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img className={classnames('padding-vert--md', styles.featureImage)} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

function Home() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Simple and Distributed Machine Learning"
      keywords={["benthos","stream processor","data engineering","ETL","ELT","event processor","go","golang"]}>
      <header className={classnames('hero', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={classnames('col col--5 col--offset-1')}>
              <h1 className="hero__title">{siteConfig.title}</h1>
              <p className="hero__subtitle">{siteConfig.tagline}</p>
              <div className={styles.buttons}>
                <Link
                  className={classnames(
                    'button button--outline button--primary button--lg',
                    styles.getStarted,
                  )}
                  to={useBaseUrl('docs/guides/getting_started')}>
                  Get Started
                </Link>
              </div>
            </div>
            <div className={classnames('col col--5')}>
              <img className={styles.heroImg} src="img/logo.svg" />
            </div>
          </div>
        </div>
      </header>
      <main>
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
            <div className={classnames(`${styles.pitch} col col--6`)}>
              <h2>It's boringly easy to use</h2>
              <p>
                Written in Go, deployed as a static binary, declarative configuration. <a href="https://github.com/Jeffail/benthos">Open source</a> and cloud native as utter heck.
              </p>
              {installs && installs.length && (
                <Tabs defaultValue={installs[0].label} values={installs.map((props, idx) => {
                  return {label:props.label, value:props.label};
                })}>
                  {installs.map((props, idx) => (
                    <TabItem value={props.label}>
                      <CodeSnippet snippet={props.snippet} lang="bash"></CodeSnippet>
                    </TabItem>
                  ))}
                </Tabs>
              )}
            </div>
            <div className={classnames('col col--6')}>
                {snippets && snippets.length && (
                  <section className={styles.configSnippets}>
                    <Tabs defaultValue={snippets[0].label} values={snippets.map((props, idx) => {
                      return {label:props.label, value:props.label};
                    })}>
                      {snippets.map((props, idx) => (
                        <TabItem value={props.label}>
                          <>
                          <Snippet key={idx} {...props} />
                          <Link
                            className={classnames('button button--outline button--secondary')}
                            to={props.further}>
                            Read more
                          </Link>
                          </>
                        </TabItem>
                      ))}
                    </Tabs>
                  </section>
                )}
            </div>
          </div>
        </div>
        <section className={styles.loveSection}>
          <div className="container">
            <div className="row">
              <div className={classnames('col col--6')}>
                <h3>Sponsored by the following heroes</h3>
                <a href="https://www.meltwater.com/" className={styles.sponsorLink}><img className={styles.meltwaterImg} src="/img/sponsors/mw_logo.png" /></a>
                <a href="https://www.humansecurity.com" className={styles.sponsorLink}><img className={styles.humanImg} src="/img/sponsors/HUMAN_logo.png" /></a>
                <a href="https://www.infosum.com/" className={styles.sponsorLink}><img className={styles.infosumImg} src="/img/sponsors/infosum_logo.png" /></a>
                <a href="https://community.com/" className={styles.sponsorLink}><img className={styles.communityImg} src="/img/sponsors/community.svg" /></a>
              </div>
              <div className={classnames('col col--6', styles.loveSectionPlea)}>
                <div>
                  <a href="https://github.com/sponsors/Jeffail">
                    <img className={styles.loveImg} src="img/blobheart.svg" alt="Blob Heart" />
                  </a>
                </div>
                <Link
                  className={classnames('button button--danger')}
                  to="https://github.com/sponsors/Jeffail">
                  Become a sponsor
                </Link>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Home;
