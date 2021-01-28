FROM mcr.microsoft.com/mmlspark/spark2.4:v4_mini
MAINTAINER Dalitso Banda <dalitsohb@gmail.com>

ENV LIVY_VERSION="git_master"
ENV LIVY_COMMIT="02550f7919b7348b6a7270cf806e031670037b2f"
ENV LOG_TAG="[LIVY_${LIVY_VERSION}]:" \
    LIVY_HOME="/livy" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

RUN echo "$LOG_TAG Install essentials" && \
    apk add --no-cache git wget curl && \
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
RUN wget http://central.maven.org/maven2/org/apache/spark/spark-hive_2.12/2.4.0/spark-hive_2.12-2.4.0.jar -P /opt/spark/jars

CMD ["sh", "-c", "echo '\nspark.driver.host' $(hostname -i) >> /opt/spark/conf/spark-defaults.conf && echo '\nlivy.spark.master' $SPARK_MASTER >> /livy/conf/livy.conf && bin/livy-server"]
