FROM mcr.microsoft.com/openjdk/jdk:11-mariner
LABEL maintainer="Dalitso Banda dalitsohb@gmail.com"

# Get Spark from US Apache mirror.
ENV APACHE_SPARK_VERSION 2.4.5
ENV HADOOP_VERSION 3.2.1

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

ADD patch_beam.patch /tmp/patch_beam.patch

ENV Z_VERSION="git_master"
ENV Z_COMMIT="2ea945f548a4e41312026d5ee1070714c155a11e"
ENV LOG_TAG="[ZEPPELIN_${Z_VERSION}]:" \
    Z_HOME="/zeppelin" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

RUN echo "$LOG_TAG Install essentials" && \
    apt-get -y update && \
    apt-get install -y locales && \
    locale-gen $LANG && \
    apt-get install -y git wget grep curl sed && \
    apt-get autoclean &&  apt-get autoremove

RUN echo "$LOG_TAG Getting maven" && \
    wget http://www.eu.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && \
    tar -zxf apache-maven-3.3.9-bin.tar.gz -C /usr/local/ && \
    ln -s /usr/local/apache-maven-3.3.9/bin/mvn /usr/local/bin/mvn

RUN echo "$LOG_TAG install nodejs" && \
    curl -sL https://deb.nodesource.com/setup_12.x | bash - && apt-get install -y nodejs && \
    echo "$LOG_TAG Download Zeppelin source" && \
    git clone https://github.com/apache/zeppelin.git /zeppelin-${Z_VERSION}-bin-all && \
    mv /zeppelin-${Z_VERSION}-bin-all ${Z_HOME}_src && \
    mkdir ${Z_HOME}/notebook/mmlspark -p && \
    cd ${Z_HOME}_src && \
    git checkout ${Z_COMMIT} && \
    echo '{ "allow_root": true }' > /root/.bowerrc && \
    echo "$LOG_TAG building zeppelin" && \
    # setup \
    cd ${Z_HOME}_src && \
    git status  && \
    mv /tmp/patch_beam.patch . && \
    git apply --ignore-space-change --ignore-whitespace patch_beam.patch && \
     ./dev/change_scala_version.sh 2.12 && \
    # dendencies
    apt-get -y update && \
    apt-get install -y git libfontconfig r-base-dev r-cran-evaluate wget grep curl sed && \
    # setup zeppelin-web
    cd ${Z_HOME}_src/zeppelin-web && \
    rm package-lock.json && \
    mkdir -p /usr/local/lib/node_modules && \
    npm install -g @angular/cli && \
    npm install -g grunt-cli bower && \
    bower install

RUN cd ${Z_HOME}_src && \
    export MAVEN_OPTS="-Xmx2048m -XX:MaxPermSize=256m" && \
    mvn -e -B package -DskipTests -Pscala-2.12 -Pbuild-distr && \
    tar xvf ${Z_HOME}_src/zeppelin-distribution/target/zeppelin-0.9.0-SNAPSHOT.tar.gz && \
    rm -rf ${Z_HOME}/* && \
    mv zeppelin-0.9.0-SNAPSHOT ${Z_HOME}_dist && \
    mv ${Z_HOME}_dist/* ${Z_HOME} && \
    echo "$LOG_TAG Cleanup" && \
    rm -rf /usr/local/apache-maven-3.3.9 && \
    npm uninstall -g @angular/cli grunt-cli bower && \
    rm -rf /usr/local/apache-maven-3.3.9 && \
    rm -rf ${Z_HOME}_dist && \
    rm -rf ${Z_HOME}_src && \
    rm -rf /root/.ivy2 && \
    rm -rf /root/.m2 && \
    rm -rf /root/.npm && \
    rm -rf /root/.cache && \
    rm -rf /tmp/*

ADD jars /jars

# add notebooks
ADD mmlsparkExamples/ ${Z_HOME}/notebook/mmlspark/

ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ADD zeppelin-env.sh ${Z_HOME}/conf/

# use python3 as default since thats what's in the base image \
RUN echo "export PYSPARK_DRIVER_PYTHON=python3" >> ${Z_HOME}/conf/zeppelin-env.sh && \
    echo "export PYSPARK_PYTHON=python3" >> ${Z_HOME}/conf/zeppelin-env.sh

EXPOSE 8080

WORKDIR ${Z_HOME}
CMD ["sh", "-c", "echo '\nspark.driver.host' $(hostname -i) >> /opt/spark/conf/spark-defaults.conf && bin/zeppelin.sh"]
