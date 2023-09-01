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
ADD spark-config/log4j.properties /opt/spark/conf/log4j.properties
ADD spark-config/start-common.sh spark-config/start-worker spark-config/start-master /
ADD spark-config/core-site.xml /opt/spark/conf/core-site.xml
ADD spark-config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PATH $PATH:/opt/spark/bin

ENV LIVY_VERSION="git_master"
ENV LIVY_COMMIT="02550f7919b7348b6a7270cf806e031670037b2f"
ENV LOG_TAG="[LIVY_${LIVY_VERSION}]:" \
    LIVY_HOME="/livy" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

RUN echo "$LOG_TAG Install essentials" && \
    apt-get update && \
    apt-get install -y git wget curl && \
    echo "$LOG_TAG setting python dependencies" && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    echo "$LOG_TAG Getting maven" && \
    wget http://www.eu.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && \
    tar -zxf apache-maven-3.3.9-bin.tar.gz -C /usr/local/ && \
    rm -rf apache-maven-3.3.9-bin.tar.gz && \
    ln -s /usr/local/apache-maven-3.3.9/bin/mvn /usr/local/bin/mvn && \
    echo "$LOG_TAG Download and build Livy source" && \
    git clone https://github.com/apache/incubator-livy.git ${LIVY_HOME}_src && \
    cd ${LIVY_HOME}_src  && \
    git checkout ${LIVY_COMMIT} && \
    mvn package -DskipTests && \
    mv ${LIVY_HOME}_src ${LIVY_HOME} && \
    echo "$LOG_TAG Cleanup" && \
    rm -rf /usr/local/apache-maven-3.3.9 && \
    rm -rf /root/.ivy2 && \
    rm -rf /root/.npm && \
    rm -rf /root/.m2 && \
    rm -rf /root/.cache && \
    rm -rf /tmp/*

ADD jars /jars

ENV HADOOP_CONF_DIR /opt/hadoop/conf
ENV CONF_DIR /livy/conf
ENV SPARK_CONF_DIR /opt/spark/conf

ADD livy.conf ${LIVY_HOME}/conf
EXPOSE 8998

WORKDIR ${LIVY_HOME}

RUN mkdir logs

#hive needed for livy pyspark
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-hive_2.12/2.4.5/spark-hive_2.12-2.4.5.jar -P /opt/spark/jars

CMD ["sh", "-c", "echo '\nspark.driver.host' $(hostname -i) >> /opt/spark/conf/spark-defaults.conf && echo '\nlivy.spark.master' $SPARK_MASTER >> /livy/conf/livy.conf && bin/livy-server"]
