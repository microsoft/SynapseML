FROM mcr.microsoft.com/mmlspark/spark2.4:v4_mini
MAINTAINER Dalitso Banda <dalitsohb@gmail.com>

ADD patch_beam.patch /tmp/patch_beam.patch

ENV Z_VERSION="git_master"
ENV Z_COMMIT="2ea945f548a4e41312026d5ee1070714c155a11e"
ENV LOG_TAG="[ZEPPELIN_${Z_VERSION}]:" \
    Z_HOME="/zeppelin" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

RUN echo "$LOG_TAG setting python dependencies" && \
    apk add --no-cache python && \
    pip install --no-cache-dir --upgrade pip setuptools && \
    rm -rf /root/.cache && \

    echo "$LOG_TAG Install essentials" && \
    apk add --no-cache git wget curl && \
    apk add --no-cache && \
    apk --no-cache --update add ca-certificates wget && \
    apk --no-cache --update add libstdc++ libstdc++6 && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.26-r0/glibc-2.26-r0.apk && \
    apk del --no-cache --update libc6-compat && \
    apk add --no-cache --update glibc-2.26-r0.apk && \
    echo "$LOG_TAG Getting maven" && \
    wget http://www.eu.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && \
    tar -zxf apache-maven-3.3.9-bin.tar.gz -C /usr/local/ && \
    rm apache-maven-3.3.9-bin.tar.gz && \
    ln -s /usr/local/apache-maven-3.3.9/bin/mvn /usr/local/bin/mvn && \
    echo "$LOG_TAG install nodejs" && \
    apk add --no-cache --update nodejs nodejs-npm && \
    echo "$LOG_TAG Download Zeppelin source" && \
    export LD_LIBRARY_PATH=/lib:/usr/lib/:$LD_LIBRARY_PATH && \
    git clone https://github.com/apache/zeppelin.git /zeppelin-${Z_VERSION}-bin-all && \
    mv /zeppelin-${Z_VERSION}-bin-all ${Z_HOME}_src && \
    mkdir ${Z_HOME}/notebook/mmlspark -p && \
    cd ${Z_HOME}_src && \
    git checkout ${Z_COMMIT} && \
    echo '{ "allow_root": true }' > /root/.bowerrc && \
    echo "$LOG_TAG building zeppelin" && \
    cd ${Z_HOME}_src && \
    git status  && \
    mv /tmp/patch_beam.patch . && \
    git apply --ignore-space-change --ignore-whitespace patch_beam.patch && \
    ./dev/change_scala_version.sh 2.12 && \
    cd ${Z_HOME}_src/zeppelin-web && \
    rm package-lock.json && \
    mkdir -p /usr/local/lib/node_modules && \
    npm update -g && \
    npm install -g -y @angular/cli && \
    npm install -g -y grunt-cli bower && \
    npm update -g && \
    bower install && \
    npm install && \
    mvn -e -B package -DskipTests -Pscala-2.12 -Pbuild-distr && \
    cd ${Z_HOME}_src && \
    export MAVEN_OPTS="-Xmx2048m -XX:MaxPermSize=256m" && \
    mvn -e -B package -DskipTests -Pscala-2.12 -Pbuild-distr && \
    tar xvf ${Z_HOME}_src/zeppelin-distribution/target/zeppelin-0.9.0-SNAPSHOT.tar.gz && \
    rm -rf ${Z_HOME}/* && \
    mv zeppelin-0.9.0-SNAPSHOT ${Z_HOME}_dist && \
    mv ${Z_HOME}_dist/* ${Z_HOME} && \
    echo "$LOG_TAG Cleanup" && \
    rm -rf /usr/local/apache-maven-3.3.9 && \
    npm uninstall -g @angular/cli grunt-cli bower && \
    apk del --no-cache --update nodejs nodejs-npm && \
    rm -rf /usr/local/apache-maven-3.3.9 && \
    rm -rf ${Z_HOME}_dist && \
    rm -rf ${Z_HOME}_src && \
    rm -rf /root/.ivy2 && \
    rm -rf /root/.m2 && \
    rm -rf /root/.npm && \
    rm -rf /root/.cache && \
    rm -rf /tmp/* && \
    echo "deleting rarely used intepretors" && \
    rm -rf ${Z_HOME}/interpreter/pig && \
    rm -rf ${Z_HOME}/interpreter/flink && \
    rm -rf ${Z_HOME}/interpreter/scio && \
    rm -rf ${Z_HOME}/interpreter/beam && \
    rm -rf ${Z_HOME}/interpreter/scalding && \
    rm -rf ${Z_HOME}/interpreter/geode && \
    rm -rf ${Z_HOME}/interpreter/ignite && \
    rm -rf ${Z_HOME}/interpreter/alluxio && \
    rm -rf ${Z_HOME}/interpreter/hazelcastjet && \
    rm -rf ${Z_HOME}/interpreter/jdbc && \
    rm -rf ${Z_HOME}/interpreter/bigquery && \
    rm -rf ${Z_HOME}/interpreter/kylin && \
    rm -rf ${Z_HOME}/interpreter/sap && \
    rm -rf ${Z_HOME}/interpreter/cassandra && \
    rm -rf ${Z_HOME}/interpreter/groovy && \
    rm -rf ${Z_HOME}/interpreter/lens && \
    rm -rf ${Z_HOME}/interpreter/neo4j && \
    rm -rf ${Z_HOME}/interpreter/livy && \
    rm -rf ${Z_HOME}/interpreter/angular && \
    rm -rf ${Z_HOME}/interpreter/hbase && \

    echo "$LOG_TAG installing python related packages" && \
    apk add --no-cache g++ python-dev python3-dev build-base wget freetype-dev libpng-dev openblas-dev && \
    pip3 install -U pip && \
    pip3 install --no-cache-dir -U pip setuptools wheel && \
    pip3 install --no-cache-dir numpy  matplotlib pyspark&& \
    pip3 uninstall -y setuptools wheel && \
    echo "$LOG_TAG Cleanup" && \
    apk del g++ python-dev python3-dev build-base freetype-dev libpng-dev openblas-dev && \
    rm -rf /root/.npm && \
    rm -rf /root/.m2 && \
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

RUN apk add --no-cache tini
ENTRYPOINT [ "/sbin/tini", "--" ]

WORKDIR ${Z_HOME}
CMD ["sh", "-c", "echo '\nspark.driver.host' $(hostname -i) >> /opt/spark/conf/spark-defaults.conf && bin/zeppelin.sh"]
