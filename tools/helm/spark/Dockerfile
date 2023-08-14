FROM mcr.microsoft.com/openjdk/jdk:11-mariner
LABEL maintainer="Dalitso Banda dalitsohb@gmail.com"

# Get Spark from US Apache mirror.
ENV APACHE_SPARK_VERSION 2.4.5
ENV HADOOP_VERSION 3.3.4

RUN echo "$LOG_TAG Getting SPARK_HOME" && \
    apt-get update && \
    # build deps and deps for c bindings for cntk
    apt-get install -y build-essential && \
    apt-get install -y autoconf automake libtool curl make unzip && \
    mkdir -p /opt && \
    cd /opt && \
    curl http://apache.claz.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-without-hadoop.tgz  | \
        tar -xz && \
    ln -s spark-${APACHE_SPARK_VERSION}-bin-without-hadoop spark && \
    echo Spark ${APACHE_SPARK_VERSION} installed in /opt/spark && \
    export SPARK_HOME=/opt/spark

RUN echo "downloading hadoop" && \
    apt-get install -y wget && \
    cd /tmp && \
    wget http://apache.claz.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O - | \
    tar -xz && \
    mv /tmp/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    echo "export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/*" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo Hadoop ${HADOOP_VERSION} installed in /opt/hadoop && \
    rm -rf /opt/hadoop/share/doc

RUN echo "\nSPARK_DIST_CLASSPATH=/jars:/jars/*:$(/opt/hadoop/bin/hadoop classpath)" >> /opt/spark/conf/spark-env.sh
ENV HADOOP_HOME=/opt/hadoop
ADD jars /jars

# if numpy is installed on a driver it needs to be installed on all
# workers, so install it everywhere
RUN apt-get update && \
    apt install -y python3-pip && \
    pip3 install numpy && \
    pip3 install matplotlib && \
    pip3 install pandas==0.24.1 && \
    pip3 install scikit-learn && \
    pip3 install pyarrow==0.11.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Final config
ADD log4j.properties /opt/spark/conf/log4j.properties
ADD start-common.sh start-worker start-master /
ADD core-site.xml /opt/spark/conf/core-site.xml
ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PATH $PATH:/opt/spark/bin
