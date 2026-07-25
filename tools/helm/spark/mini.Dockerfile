#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM mcr.microsoft.com/openjdk/jdk:11-mariner

ARG spark_jars=jars
ARG img_path=kubernetes/dockerfiles
ARG k8s_tests=kubernetes/tests


# Get Spark from US Apache mirror.
ENV APACHE_SPARK_VERSION 2.4.3
ENV HADOOP_VERSION 3.3.4
ENV HADOOP_GIT_COMMIT="release-3.2.0-RC1"

ENV SPARK_HOME=/opt/spark


RUN set -ex && \
    apk upgrade --no-cache && \
    apk add --no-cache bash tini libc6-compat linux-pam && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/work-dir && \
    echo "$LOG_TAG Getting SPARK_HOME" && \
    mkdir -p /opt && \
    cd /tmp && \
    wget http://apache.claz.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-without-hadoop.tgz -O - | \
        tar -xz && \
    mkdir -p /opt/spark && \
    mv spark-${APACHE_SPARK_VERSION}-bin-without-hadoop /tmp/spark_bin && \
    echo Spark ${APACHE_SPARK_VERSION} installed in /opt/spark && \

    cp -r /tmp/spark_bin/${spark_jars} /opt/spark/jars && \
    cp -r /tmp/spark_bin/bin /opt/spark/bin && \
    cp -r /tmp/spark_bin/sbin /opt/spark/sbin && \
    cp -r /tmp/spark_bin/${img_path}/spark/entrypoint.sh /opt/ && \
    cp -r /tmp/spark_bin/examples /opt/spark/examples && \
    cp -r /tmp/spark_bin/${k8s_tests} /opt/spark/tests && \
    cp -r /tmp/spark_bin/data /opt/spark/data && \
    cp -r /tmp/spark_bin/python /opt/spark/python && \

    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -r /tmp/spark_bin && \

    echo "downloading hadoop" && \
    cd /tmp && \
    wget http://apache.claz.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O - | \
    tar -xz && \
    mv /tmp/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    echo "export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/*" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo Hadoop ${HADOOP_VERSION} installed in /opt/hadoop && \
    rm -rf /opt/hadoop/share/doc

ENV HADOOP_HOME=/opt/hadoop
RUN mkdir -p /opt/spark/conf && \
    echo "SPARK_DIST_CLASSPATH=/jars:/jars/*:$(/opt/hadoop/bin/hadoop classpath)" >> /opt/spark/conf/spark-env.sh


RUN apk add --no-cache python3 && \
    pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache

# # if numpy is installed on a driver it needs to be installed on all
# # workers, so install it everywhere
# RUN apt-get update && \
#     apt-get install -y g++ python-dev build-essential python3-dev && \
#     curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
#     python get-pip.py && \
#     rm get-pip.py && \
#     pip install -U pip setuptools wheel && \
#     pip install numpy && \
#     pip install matplotlib && \
#     pip install pandas && \
#     apt-get purge -y --auto-remove python-dev build-essential python3-dev && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

ADD log4j.properties /opt/spark/conf/log4j.properties
ADD start-common.sh start-worker start-master /
ADD core-site.xml /opt/spark/conf/core-site.xml
ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PATH $PATH:/opt/spark/bin
