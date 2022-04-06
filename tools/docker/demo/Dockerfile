FROM ubuntu:20.04

ARG MMLSPARK_VERSION

ENV SPARK_VERSION=3.2.1
ENV HADOOP_VERSION=2.7
ENV MMLSPARK_VERSION=${MMLSPARK_VERSION}

RUN apt-get -qq update \
    && apt-get -qq -y install curl bzip2 wget default-jre default-jdk \
    && curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda install -y python=3 \
    && conda install -c conda-forge \
    && conda update conda \
    && apt-get -qq -y remove curl bzip2 \
    && apt-get -qq -y autoremove \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log \
    && conda clean --all --yes

# Downloads Spark
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Spark and move it to the folder "/opt/spark"
RUN tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME /opt/spark
ENV PATH /opt/conda/bin:$PATH
ENV JAVA_HOME /usr/lib/jvm/java-1.11.0-openjdk-amd64

# Build-essentials
# RUN apt-get update \
# 	&& apt-get install -y build-essential

# Open-MPI installation (required to use spark on AML)
# ENV OPENMPI_VERSION 3.1.2
# RUN mkdir /tmp/openmpi && \
#     cd /tmp/openmpi && \
#     wget https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-${OPENMPI_VERSION}.tar.gz && \
#     tar zxf openmpi-${OPENMPI_VERSION}.tar.gz && \
#     cd openmpi-${OPENMPI_VERSION} && \
#     ./configure --enable-orterun-prefix-by-default && \
#     make -j $(nproc) all && \
#     make install && \
#     ldconfig && \
#     rm -rf /tmp/openmpi

RUN conda install jupyter \
    && jupyter-notebook --generate-config \
    && ipython profile create \
    && echo "c.NotebookApp.token = ''" >> ~/.jupyter/jupyter_notebook_config.py \
    && echo "c.NotebookApp.ip = '0.0.0.0'" >> ~/.jupyter/jupyter_notebook_config.py \
    && echo "c.NotebookApp.allow_root = True" >> ~/.jupyter/jupyter_notebook_config.py \
    && conda clean --all --yes

# Copy the init script for jupyter startup.
COPY tools/docker/demo/init_notebook.py /root/.ipython/profile_default/startup/init_notebook.py
COPY notebooks notebooks
COPY . src
WORKDIR notebooks/features

# Jupyter Notebook UI
EXPOSE 8888

# Synapse UI
EXPOSE 4040

# Conda create environment for jupyter
# WORKDIR src
# RUN conda env create -f=environment.yaml -n synapseml
