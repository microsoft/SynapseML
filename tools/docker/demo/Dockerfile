FROM mcr.microsoft.com/oss/mirror/docker.io/library/ubuntu:20.04

ARG SYNAPSEML_VERSION=0.11.0
ARG DEBIAN_FRONTEND=noninteractive

ENV SPARK_VERSION=3.3.1
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
RUN curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda update conda \
    && conda install -y python=3 jupyter pyspark \
    && conda clean --all --yes

ENV PATH /opt/conda/bin:$PATH

# Download and install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

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
COPY notebooks notebooks
WORKDIR notebooks/features

# Jupyter Notebook UI
EXPOSE 8888

# Synapse UI
EXPOSE 4040
