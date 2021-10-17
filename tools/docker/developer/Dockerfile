FROM ubuntu:18.04

RUN apt-get -qq update \
    && apt-get -qq upgrade \
    && apt-get -qq -y install curl bzip2 \
    && curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda install -y python=3 \
    && conda update conda \
    && apt-get install openjdk-8-jdk -qq \
    && apt-get install apt-transport-https gnupg -yqq \
    && echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
    && echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list \
    && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import \
    && chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg \
    && apt-get update \
    && apt-get install sbt -qq \
    && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && apt-get -qq remove bzip2 \
    && apt-get -qq autoremove \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log \
    && conda clean --all --yes

ENV PATH /opt/conda/bin:$PATH
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64

RUN mkdir -p /__w/1
WORKDIR /__w/1

RUN git clone https://github.com/Microsoft/SynapseML.git s

WORKDIR /__w/1/s

RUN conda env create -f environment.yaml && conda clean --all --yes

ENV SBT_OPTS="${SBT_OPTS} -Dsbt.io.jdktimestamps=true"
RUN sbt setup
RUN conda init bash
RUN echo "source activate synapseml" > ~/.bashrc

# RUN /bin/bash -c "sbt testPython"
