FROM mcr.microsoft.com/mirror/docker/library/ubuntu:22.04

ARG SYNAPSEML_VERSION=1.1.3
ARG DEBIAN_FRONTEND=noninteractive

ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
ENV SYNAPSEML_VERSION=${SYNAPSEML_VERSION}
ENV JAVA_HOME /usr/lib/jvm/java-1.11.0-openjdk-amd64

# Install required packages
RUN apt-get -qq update \
    && apt-get -qq -y install \
      curl \
      bzip2 \
      wget \
      default-jre \
      openmpi-bin \
    && apt-get upgrade -y \
    && apt-get clean -y \
    && apt-get -qq -y autoremove \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log

# Install conda and required dependencies
RUN curl -sSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda config --set auto_activate_base false \
    && conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main \
    && conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r \
    && conda update -y conda \
    && conda install -y python=3 jupyter \
    && pip install --upgrade "PyJWT>=2.12.0" \
    && conda clean --all --yes

ENV PATH /usr/local/bin:$PATH

# Download and install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Patch netty 4.1.96.Final (CVE-2023-44487, CVE-2024-29025, CVE-2025-24970, ...) to 4.1.118.Final.
# Spark 3.5.x pins netty 4.1.96 upstream; we override in-place since 4.1.x is binary-compatible.
ENV NETTY_VERSION=4.1.118.Final
RUN cd /opt/spark/jars \
    && rm -f netty-*-4.1.96.Final*.jar \
    && for c in all buffer codec codec-http codec-http2 codec-socks common handler handler-proxy resolver transport transport-classes-epoll transport-classes-kqueue transport-native-unix-common; do \
         curl -fsSLO "https://repo1.maven.org/maven2/io/netty/netty-${c}/${NETTY_VERSION}/netty-${c}-${NETTY_VERSION}.jar"; \
       done \
    && for cls in linux-x86_64 linux-aarch_64; do \
         curl -fsSLO "https://repo1.maven.org/maven2/io/netty/netty-transport-native-epoll/${NETTY_VERSION}/netty-transport-native-epoll-${NETTY_VERSION}-${cls}.jar"; \
       done \
    && for cls in osx-x86_64 osx-aarch_64; do \
         curl -fsSLO "https://repo1.maven.org/maven2/io/netty/netty-transport-native-kqueue/${NETTY_VERSION}/netty-transport-native-kqueue-${NETTY_VERSION}-${cls}.jar"; \
       done

ENV SPARK_HOME /opt/spark
ENV PYTHONPATH $SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j*:$PYTHON_PATH
ENV PATH $SPARK_HOME/bin/:$SPARK_HOME/python/:$PATH

RUN apt-get remove --purge -y \
        curl \
        bzip2 \
        wget \
    && apt-get -qq -y autoremove \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log

# Setup Jupyter
RUN jupyter-notebook --generate-config \
    && ipython profile create \
    && echo "c.NotebookApp.token = ''" >> ~/.jupyter/jupyter_notebook_config.py \
    && echo "c.NotebookApp.ip = '0.0.0.0'" >> ~/.jupyter/jupyter_notebook_config.py \
    && echo "c.NotebookApp.allow_root = True" >> ~/.jupyter/jupyter_notebook_config.py \
    && conda clean --all --yes

# Copy the init script for jupyter startup.
COPY tools/docker/demo/init_notebook.py /root/.ipython/profile_default/startup/init_notebook.py
COPY docs docs
WORKDIR docs

# Jupyter Notebook UI
EXPOSE 8888

# Synapse UI
EXPOSE 4040
